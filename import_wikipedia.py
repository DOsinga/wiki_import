#!/usr/bin/env python

import argparse
import subprocess
import xml.sax

import mwparserfromhell
import psycopg2

CAT_PREFIX = 'Category:'
INFOBOX_PREFIX = 'infobox '

def setup_db(connection_string):
  conn = psycopg2.connect(connection_string)
  cursor = conn.cursor()
  cursor.execute('DROP TABLE IF EXISTS wikipedia')
  cursor.execute('CREATE TABLE wikipedia ('
                 '    title TEXT PRIMARY KEY,'
                 '    infobox TEXT,'
                 '    wikitext TEXT,'
                 '    templates TEXT[] NOT NULL DEFAULT \'{}\','
                 '    categories TEXT[] NOT NULL DEFAULT \'{}\','
                 '    general TEXT[] NOT NULL DEFAULT \'{}\''
                 ')')
  cursor.execute('CREATE INDEX wikipedia_infobox ON wikipedia(infobox)')
  cursor.execute('CREATE INDEX wikipedia_templates ON wikipedia USING gin(templates)')
  cursor.execute('CREATE INDEX wikipedia_categories ON wikipedia USING gin(categories)')
  cursor.execute('CREATE INDEX wikipedia_general ON wikipedia USING gin(general)')

  return conn, cursor


def make_tags(iterable):
  return list(set(x.strip().lower() for x in iterable))


class WikiXmlHandler(xml.sax.handler.ContentHandler):
  def __init__(self, cursor):
    xml.sax.handler.ContentHandler.__init__(self)
    self._db_cursor = cursor
    self._count = 0
    self.reset()

  def reset(self):
    self._buffer = []
    self._state = None
    self._values = {}

  def startElement(self, name, attrs):
    if name in ('title', 'text'):
      self._state = name

  def endElement(self, name):
    if name == self._state:
      self._values[name] = ''.join(self._buffer)
      self._state = None
      self._buffer = []

    if name == 'page':
      try:
        wikicode = mwparserfromhell.parse(self._values['text'])
        templates = make_tags(unicode(template.name) for template in wikicode.filter_templates())
        infobox = None
        for template in templates:
          if template.startswith(INFOBOX_PREFIX):
            infobox = template[len(INFOBOX_PREFIX):]
            break
        if len(infobox or '') > 1024 or len(self._values['title']) > 1024:
          print 'Too long'
          raise mwparserfromhell.parser.ParserError('too long')
        categories = make_tags(l.title[len(CAT_PREFIX):] for l in wikicode.filter_wikilinks() if l.title.startswith(CAT_PREFIX))
        general = make_tags(x.replace(' of ', ' in ').split(' in ')[0] for x in categories if ' of ' in x or ' in 'in x)
        self._db_cursor.execute('INSERT INTO wikipedia (title, infobox, wikitext, templates, categories, general) VALUES (%s, %s, %s, %s, %s, %s)',
                                (self._values['title'], infobox, self._values['text'], templates, categories, general))
        self._count += 1
        print self._count
        if self._count % 100 == 0:
          print self._count
        if self._count % 100000 == 0:
          raise StopIteration
      except mwparserfromhell.parser.ParserError:
        print 'mwparser error for:', self._values['title']
      self.reset()

  def characters(self, content):
    if self._state:
      self._buffer.append(content)


def main(dump, cursor):
  parser = xml.sax.make_parser()
  parser.setContentHandler(WikiXmlHandler(cursor))
  for line in subprocess.Popen(['bzcat'], stdin=file(dump), stdout=subprocess.PIPE).stdout:
    try:
      parser.feed(line)
    except StopIteration:
      break


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Import wikipedia into postgress')
  parser.add_argument('--postgres', type=str,
                      help='postgres connection string')
  parser.add_argument('dump', type=str,
                      help='BZipped wikipedia dump')

  args = parser.parse_args()
  conn, cursor = setup_db(args.postgres)

  main(args.dump, cursor)

  conn.commit()

