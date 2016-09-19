#!/usr/bin/env python

import unittest
import xml

import re
from import_wikipedia import WikiXmlHandler, extact_general

DUMP = """<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.10/ http://www.mediawiki.org/xml/export-0.10.xsd" version="0.10" xml:lang="en">
  <siteinfo>
    <sitename>Wikipedia</sitename>
    <dbname>enwiki</dbname>
    <base>https://en.wikipedia.org/wiki/Main_Page</base>
    <generator>MediaWiki 1.28.0-wmf.15</generator>
    <case>first-letter</case>
    <namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
    </namespaces>
  </siteinfo>
  <page>
    <title>AccessibleComputing</title>
    <ns>0</ns>
    <id>10</id>
    <redirect title="Computer accessibility" />
    <revision>
      <id>631144794</id>
      <parentid>381202555</parentid>
      <timestamp>2014-10-26T04:50:23Z</timestamp>
      <contributor>
        <username>Paine Ellsworth</username>
        <id>9092818</id>
      </contributor>
      <comment>add [[WP:RCAT|rcat]]s</comment>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text xml:space="preserve">#REDIRECT [[Computer accessibility]]

{{Redr|move|from CamelCase|up}}</text>
      <sha1>4ro7vvppa5kmm0o1egfjztzcwd0vabw</sha1>
    </revision>
  </page>
  <page>
    <title>Anarchism</title>
    <ns>0</ns>
    <id>12</id>
    <revision>
      <id>734566960</id>
      <timestamp>2016-08-15T06:01:51Z</timestamp>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text xml:space="preserve">{{Redirect2|Anarchist|Anarchists|the fictional character|Anarchist (comics)|other uses|Anarchists (disambiguation)}}
{{Basic forms of government}}

'''Anarchism''' is a [[political philosophy]] that advocates [[self-governance|self-governed]] societies based on voluntary institutions. These are often described

&lt;--This is a *citation* from a book, DON'T CHANGE--&gt;

===First International and the Paris Commune===
{{Main article|International Workingmen's Association|Paris Commune}}
[[File:Bakunin.png|thumb|upright|Collectivist anarchist [[Mikhail Bakunin]] opposed the

[[Category:Anti-fascism]]
[[Category:Ideas of idealists]]
[[Category:Anti-capitalism]]
[[Category:Far-left politics]]</text>
      <sha1>az60vahaazg403faw6x2gzpbmiws0o3</sha1>
    </revision>
  </page>
</mediawiki>"""


RE_PAR = re.compile('\(([^\)]+)\)')

class FakeCursor():
  def __init__(self):
    self.results = []

  def execute(self, sql, params):
    g = RE_PAR.search(sql)
    fields = [x.strip() for x in g.group(1).split(',')]
    self.results.append(dict(zip(fields, params)))


class TestImportWikipedia(unittest.TestCase):
  def test_parse_wikipedia(self):
    parser = xml.sax.make_parser()
    fc = FakeCursor()
    parser.setContentHandler(WikiXmlHandler(fc))
    for line in DUMP.split('\n'):
      parser.feed(line + '\n')

    self.assertEqual(len(fc.results), 2)

    self.assertEqual(fc.results[0]['title'], 'AccessibleComputing')
    self.assertTrue('redr' in fc.results[0]['templates'])
    self.assertTrue("<--This is a *citation* from a book, DON'T CHANGE-->" in fc.results[1]['wikitext'])
    self.assertTrue('main article' in fc.results[1]['templates'])
    self.assertTrue('ideas' in fc.results[1]['general'])

  def test_extact_general(self):
    self.assertEqual(extact_general('something something dark'), None)
    self.assertEqual(extact_general('the streets of philadelpha'), 'the streets')
    self.assertEqual(extact_general('paintings by dutch potato eaters'), 'paintings')
    self.assertEqual(extact_general('Cities in trouble'), 'Cities')

if __name__ == '__main__':
  unittest.main()
