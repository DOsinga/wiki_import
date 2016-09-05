#!/usr/bin/env python

import json
import unittest
from import_wikidata import parse_wikidata, map_value

class TestImportWikidata(unittest.TestCase):
  def test_parse_wikidata(self):
    objs = [{'hello': 'world'}, {'all': 'ok?'}, {'or': ['something', 'with', 'more']}]
    lines = ['[\n']
    for idx, obj in enumerate(objs):
      lines.append(json.dumps(obj) + (',' if idx < len(objs) -1 else '') + '\n')
    lines.append(']\n')
    parsed = list(parse_wikidata(lines))
    self.assertEqual(parsed, objs)

  def test_map_value(self):
    coo = {'value': {
              'latitude': 52,
              'longitude': 13,
              'altitude': None,
              'precision': 0.016666666666667,
              'globe': 'http://www.wikidata.org/entity/Q2'
            },
            'type': 'globecoordinate'}
    self.assertEqual(map_value(coo, {}), {'lat':52, 'lng': 13})
    time = {'value': {
              'time': '+2001-12-00T00:00:00Z',
              'timezone': 0,
              'before': 0,
              'after': 0,
              'precision': 11,
              'calendarmodel': 'http:\/\/www.wikidata.org\/entity\/Q1985727'
            },
            'type': 'time'}
    self.assertEqual(map_value(time, {}), '2001-12-01T00:00:00')

if __name__ == '__main__':
  unittest.main()
