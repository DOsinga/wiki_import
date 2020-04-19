#!/usr/bin/env python

import argparse
import subprocess
import xml.sax
from collections import OrderedDict

import mwparserfromhell
import psycopg2
import re

CAT_PREFIX = 'Category:'
INFOBOX_PREFIX = 'infobox '

RE_GENERAL = re.compile('(.+?)(\ (in|of|by)\ )(.+)')


def setup_db(connection_string):
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS wikipedia')
    cursor.execute(
        'CREATE TABLE wikipedia ('
        '    title TEXT PRIMARY KEY,'
        '    wiki_id INTEGER,'
        '    infobox TEXT,'
        '    wikitext TEXT,'
        '    templates TEXT[] NOT NULL DEFAULT \'{}\','
        '    categories TEXT[] NOT NULL DEFAULT \'{}\','
        '    general TEXT[] NOT NULL DEFAULT \'{}\','
        '    lng_lat GEOGRAPHY(POINT,4326)'
        ')'
    )
    cursor.execute('CREATE INDEX wikipedia_infobox ON wikipedia(infobox)')
    cursor.execute('CREATE INDEX wikipedia_templates ON wikipedia USING gin(templates)')
    cursor.execute('CREATE INDEX wikipedia_categories ON wikipedia USING gin(categories)')
    cursor.execute('CREATE INDEX wikipedia_general ON wikipedia USING gin(general)')
    cursor.execute('CREATE INDEX wikipedia_lng_lat ON wikipedia USING GIST(lng_lat)')

    return conn, cursor


def make_tags(iterable):
    return list(set(x.strip().lower() for x in iterable if x and len(x) < 256))


def strip_template_name(name):
    return name.strip_code().strip()


def extact_general(category):
    m = RE_GENERAL.match(category)
    if m:
        return m.groups()[0]
    return None


def parse_coordinate(template):
    lat = None
    lng = None
    val = 0.0
    multiplier = 1.0
    params = [
        param.value.strip_code().strip().upper()
        for param in template.params
        if param.value and not param.showkey and not ':' in param.value
    ]
    if len(params) == 2:
        try:
            return map(float, params)
        except ValueError:
            print('error converting to float', params)
            return None, None
    for param in params:
        if param in 'NSEW':
            if param in 'SW':
                val = -val
            if param in 'NS':
                lat = val
            else:
                lng = val
            val = 0
            multiplier = 1.0
        else:
            try:
                v = float(param)
            except ValueError:
                continue
            val += v * multiplier
            multiplier /= 60.0
    return lat, lng


class WikiXmlHandler(xml.sax.handler.ContentHandler):
    def __init__(self, cursor, record_limit):
        xml.sax.handler.ContentHandler.__init__(self)
        self._db_cursor = cursor
        self._count = 0
        self._lng_lat = 0
        self._record_limit = record_limit
        self.reset()

    def reset(self):
        self._buffer = []
        self._state = None
        self._values = {}

    def startElement(self, name, attrs):
        if name in ('title', 'text', 'id'):
            self._state = name

    def endElement(self, name):
        if name == self._state:
            self._values[name] = ''.join(self._buffer)
            self._state = None
            self._buffer = []

        if name == 'page':
            try:
                wikicode = mwparserfromhell.parse(self._values['text'])
                template_dict = OrderedDict(
                    (strip_template_name(template.name), template) for template in wikicode.filter_templates()
                )
                lat = lng = None
                for template_name, template in template_dict.items():
                    if template_name.lower() in ('coord missing', 'coord unknown'):
                        continue
                    if any(template_name.lower().startswith(prefix) for prefix in ('coor', 'geolinks')):
                        try:
                            lat, lng = parse_coordinate(template)
                        except ValueError:
                            continue
                        if lat and lng:
                            break

                if lat and lng:
                    self._lng_lat += 1
                templates = make_tags(template_dict.keys())
                infobox = None
                for template in templates:
                    if template.startswith(INFOBOX_PREFIX):
                        infobox = template[len(INFOBOX_PREFIX) :]
                        break
                if len(infobox or '') > 1024 or len(self._values['title']) > 1024:
                    raise mwparserfromhell.parser.ParserError('too long')
                categories = make_tags(
                    l.title[len(CAT_PREFIX) :] for l in wikicode.filter_wikilinks() if l.title.startswith(CAT_PREFIX)
                )
                general = make_tags(extact_general(x) for x in categories)

                to_insert = (
                    self._values['title'],
                    int(self._values['id']),
                    infobox,
                    self._values['text'],
                    templates,
                    categories,
                    general,
                )
                if lat is None or lng is None:
                    place_holder = 'NULL'
                else:
                    to_insert += (lng, lat)
                    place_holder = "ST_GeographyFromText('SRID=4326;POINT(%s %s)')"
                # even though we shouldn't get dupes, sometimes wikidumps are faulty:
                sql = (
                    "INSERT INTO wikipedia "
                    "(title, wiki_id, infobox, wikitext, templates, categories, general, lng_lat) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, " + place_holder + ") "
                    "ON CONFLICT DO NOTHING"
                )

                self._db_cursor.execute(sql, to_insert)
                self._count += 1
                if self._count % 100000 == 0:
                    print(self._count, self._lng_lat)
                if self._record_limit and self._count >= self._record_limit:
                    raise StopIteration
            except mwparserfromhell.parser.ParserError:
                print('mwparser error for:', self._values['title'])

            self.reset()

    def characters(self, content):
        if self._state:
            self._buffer.append(content)


def main(dump, cursor, record_limit):
    parser = xml.sax.make_parser()
    parser.setContentHandler(WikiXmlHandler(cursor, record_limit))
    for line in subprocess.Popen(['bzcat'], stdin=open(dump), stdout=subprocess.PIPE).stdout:
        try:
            parser.feed(line)
        except StopIteration:
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import wikipedia into postgress')
    parser.add_argument('--postgres', type=str, help='postgres connection string')
    parser.add_argument('--record_limit', type=int, default=0, help='if larger than 0, import only so many records')
    parser.add_argument('dump', type=str, help='BZipped wikipedia dump')

    args = parser.parse_args()
    conn, cursor = setup_db(args.postgres)

    main(args.dump, cursor, args.record_limit)

    conn.commit()
