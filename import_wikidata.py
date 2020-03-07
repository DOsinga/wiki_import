#!/usr/bin/env python

from collections import defaultdict

import argparse
import subprocess
import json

import psycopg2
from psycopg2 import extras


def setup_db(connection_string):
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS wikidata')
    cursor.execute(
        'CREATE TABLE wikidata ('
        '    wikipedia_id TEXT PRIMARY KEY,'
        '    title TEXT,'
        '    wikidata_id TEXT,'
        '    description TEXT,'
        '    properties JSONB'
        ')'
    )
    cursor.execute('CREATE INDEX wikidata_wikidata_id ON wikidata(wikidata_id)')
    cursor.execute('CREATE INDEX wikidata_properties ON wikidata USING gin(properties)')
    return conn, cursor


def parse_wikidata(lines):
    for line in lines:
        line = line.strip()
        if line and line[0] == '{':
            if line[-1] == ',':
                line = line[:-1]
            yield json.loads(line)


def map_value(value, id_name_map):
    if not value or not 'type' in value or not 'value' in value:
        return None
    typ = value['type']
    value = value['value']
    if typ == 'string':
        return value
    elif typ == 'wikibase-entityid':
        entitiy_id = value['id']
        return id_name_map.get(entitiy_id)
    elif typ == 'time':
        time_split = DATE_PARSE_RE.match(value['time'])
        if not time_split:
            return None
        year, month, day, hour, minute, second = map(int, time_split.groups())
        if day == 0:
            day = 1
        if month == 0:
            month = 1
        return '%04d-%02d-%02dT%02d:%02d:%02d' % (year, month, day, hour, minute, second)
    elif typ == 'quantity':
        return float(value['amount'])
    elif typ == 'monolingualtext':
        return value['text']
    elif typ == 'globecoordinate':
        lat = value.get('latitude')
        lng = value.get('longitude')
        if lat or lng:
            res = {'lat': lat, 'lng': lng}
            globe = value.get('globe', '').rsplit('/', 1)[-1]
            if globe != 'Q2' and globe in id_name_map:
                res['globe'] = globe
            if value.get('altitude'):
                res['altitude'] = value['altitude']
            return res

    return None


def main(dump, cursor):
    """We do two scans:
     - first collect the id -> name / wikipedia title
     - then store the actual objects with a json property.
     The first step takes quite a bit of memory (5Gb) - could possibly be done using a temporary table in postgres.
  """
    c = 0
    skip = 0
    id_name_map = {}
    for d in parse_wikidata(subprocess.Popen(['bzcat'], stdin=file(dump), stdout=subprocess.PIPE).stdout):
        c += 1
        if c % 1000 == 0:
            print c, skip
        if d.get('sitelinks') and d['sitelinks'].get('enwiki'):
            value = d['sitelinks']['enwiki']['title']
        elif d['labels'].get('en'):
            value = id_name_map[d['id']] = d['labels']['en']['value']
        else:
            skip += 1
            continue
        id_name_map[d['id']] = value

    wp_ids = set()
    c = 0
    rec = 0
    dupes = 0
    for d in parse_wikidata(subprocess.Popen(['bzcat'], stdin=file(dump), stdout=subprocess.PIPE).stdout):
        c += 1
        if c % 1000 == 0:
            print c, rec, dupes
        wikipedia_id = d.get('sitelinks', {}).get('enwiki', {}).get('title')
        title = d['labels'].get('en', {}).get('value')
        description = d['descriptions'].get('en', {}).get('value')
        wikidata_id = d['id']
        properties = {}
        if wikipedia_id and title:
            # There are some duplicate wikipedia_id's in there. We could make wikidata_id the primary key
            # but that doesn't fix the underlying dupe
            if wikipedia_id in wp_ids:
                dupes += 1
                continue
            wp_ids.add(wikipedia_id)
            # Properties are mapped in a way where we create lists as values for wiki entities if there is more
            # than one value. For other types, we always pick one value. If there is a preferred value, we'll
            # pick that one.
            # Mostly this does what you want. For filtering on colors for flags it alllows for the query:
            #   SELECT title FROM wikidata WHERE properties @> '{"color": ["Green", "Red", "White"]}'
            # However, if you'd want all flags that have Blue in them, you'd have to check for just "Blue"
            # and also ["Blue"].
            for prop_id, claims in d['claims'].items():
                prop_name = id_name_map.get(prop_id)
                if prop_name:
                    ranks = defaultdict(list)
                    for claim in claims:
                        mainsnak = claim.get('mainsnak')
                        if mainsnak:
                            data_value = map_value(mainsnak.get('datavalue'), id_name_map)
                            if data_value:
                                lst = ranks[claim['rank']]
                                if mainsnak['datavalue'].get('type') != 'wikibase-entityid':
                                    del lst[:]
                                lst.append(data_value)
                    for r in 'preferred', 'normal', 'depricated':
                        value = ranks[r]
                        if value:
                            if len(value) == 1:
                                value = value[0]
                            else:
                                value = sorted(value)
                            properties[prop_name] = value
                            break

            rec += 1
            cursor.execute(
                'INSERT INTO wikidata (wikipedia_id, title, wikidata_id, description, properties) VALUES (%s, %s, %s, %s, %s)',
                (wikipedia_id, title, wikidata_id, description, extras.Json(properties)),
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import wikidata into postgress')
    parser.add_argument('--postgres', type=str, help='postgres connection string')
    parser.add_argument('dump', type=str, help='BZipped wikipedia dump')

    args = parser.parse_args()
    conn, cursor = setup_db(args.postgres)

    main(args.dump, cursor)

    conn.commit()
