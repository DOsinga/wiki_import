#!/usr/bin/env python

import ahocorasick
import argparse
import json
import pickle
from collections import Counter
from itertools import chain

import psycopg2
from shapely import wkt

COUNTRIES = {'member states', 'countries'}
CITIES = {'populated places', 'cities', 'populated places established'}


def main(cursor, model):
    top_places_sql = (
        "select wikipedia.title, ST_AsText(wikipedia.lng_lat), wikipedia.general, wikistats.viewcount "
        "from wikipedia join wikistats on wikipedia.title = wikistats.title "
        "where not wikipedia.lng_lat is null and wikipedia.general && %s "
        "order by wikistats.viewcount "
        "desc limit 50000"
    )
    cursor.execute(top_places_sql, (list(COUNTRIES | CITIES),))
    info = {}
    for idx, (name, lng_lat, general, viewcount) in enumerate(cursor):
        if name in info:
            continue
        p = name.find(',')
        if p != -1:
            name = name[:p]
        as_point = wkt.loads(lng_lat)
        info[name.lower()] = (as_point.y, as_point.x, idx)

    if model:
        with open(model, 'wt') as fout:
            json.dump(info, fout, indent=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Geocode strings using wiki name references')
    parser.add_argument('--postgres', type=str, help='postgres connection string')
    parser.add_argument('--model', type=str, default='', help='If set, save the model here')

    args = parser.parse_args()
    conn = psycopg2.connect(args.postgres)
    cursor = conn.cursor()

    main(cursor, args.model)
