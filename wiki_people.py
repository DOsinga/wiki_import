import argparse
import json
import os
import re
from collections import Counter, defaultdict
import pycountry

import geopandas as gpd
import mwparserfromhell
import psycopg2
import psycopg2.extras
import yaml
from shapely import wkt


WORD_RE = re.compile(r'\w+')
CAT_PREFIX = 'Category:'
DIED_POSTFIX = ' deaths'
BIRTH_POSTFIX = ' births'


def setup_db(connection_string):
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute('DROP TABLE IF EXISTS wikitrends')
    cursor.execute('CREATE TABLE wikitrends ('
                   '    person_name TEXT PRIMARY KEY,'
                   '    view_count INTEGER,'
                   '    word_count INTEGER,'
                   '    year_born INTEGER,'
                   '    year_died INTEGER, '
                   '    gender TEXT,'
                   '    field TEXT,'
                   '    country_code TEXT,'
                   '    continent TEXT'
                   ')')
    cursor.execute('CREATE INDEX wikitrends_view_count ON wikitrends(view_count)')
    cursor.execute('CREATE INDEX wikitrends_word_count ON wikitrends(word_count)')
    cursor.execute('CREATE INDEX wikitrends_year_born ON wikitrends(year_born)')
    cursor.execute('CREATE INDEX wikitrends_gender ON wikitrends(gender)')
    cursor.execute('CREATE INDEX wikitrends_country_code ON wikitrends(country_code)')
    cursor.execute('CREATE INDEX wikitrends_continent ON wikitrends(continent)')
    cursor.execute('CREATE INDEX wikitrends_field ON wikitrends(field)')

    return conn, cursor


def tolerant_int(s):
    try:
        return int(re.sub("\D", "", s))
    except ValueError:
        return -1


def parse_person(rec):
    wikitext = rec['wikitext']
    parsed = mwparserfromhell.parse(wikitext)

    words = [w.lower() for w in WORD_RE.findall(parsed.strip_code())]
    word_count = len(words)
    word_counts = Counter(words)
    gender_words = {w: word_counts[w] for w in ('him', 'his', 'he', 'her', 'she')}

    res = {}
    for template in parsed.filter_templates():
        if template.name.lower().startswith('infobox'):
            for param in template.params:
                res[param.name.strip().lower()] = param.value
    wikilinks = [str(x.title) for x in parsed.filter_wikilinks()]
    locations = []
    for k in 'birth_place', 'death_place':
        if k in res:
            locations += [str(x.title) for x in res[k].filter_wikilinks()]

    born = None
    died = None
    for wl in parsed.filter_wikilinks():
        title = str(wl.title)
        if title.startswith(CAT_PREFIX):
            if title.endswith(BIRTH_POSTFIX):
                born = tolerant_int(title[len(CAT_PREFIX): -len(BIRTH_POSTFIX)])
            if title.endswith(DIED_POSTFIX):
                died = tolerant_int(title[len(CAT_PREFIX): -len(DIED_POSTFIX)])

    return {'person_name': rec['title'],
            'wiki_id': rec['wiki_id'],
            'infobox': rec['infobox'],
            'locations': locations,
            'word_count': word_count,
            'gender_words': gender_words,
            'view_count': rec['viewcount'],
            'wikilinks': wikilinks,
            'born': born,
            'died': died}

def fetch_people(json_dir, cursor, max_year, min_year):
    all_people = []
    for year in range(min_year, max_year):
        print(year)
        json_path = os.path.join(json_dir, str(year) + '.json')
        if os.path.isfile(json_path):
            people = json.load(open(json_path))
        else:
            cat = '%d births' % year
            q = ("SELECT wikipedia.*, wikistats.viewcount "
                 "FROM wikipedia LEFT JOIN wikistats ON wikipedia.title = wikistats.title "
                 "WHERE categories @> ARRAY['%s']") % cat
            cursor.execute(q)
            people = []
            for r in cursor.fetchall():
                if len(people) % 100 == 0:
                    print(' ', r['title'])
                people.append(parse_person(r))
            with open(json_path, 'w') as fout:
                json.dump(people, fout, indent=2)
        all_people += people
    return all_people


def assign_genders(people):
    weights = {'he': 2, 'his': 1, 'him': 1, 'she': -2, 'her': -1}

    for p in people:
        gw = p['gender_words']
        g = sum(weights[w] * gw[w] for w in weights)
        s = sum(abs(weights[w]) * gw[w] for w in weights)
        if s > 0:
            g /= s
        p['gender'] = g

    gender_by_first_name = defaultdict(list)
    for p in people:
        first_name = p['person_name'].split(' ', 1)[0]
        p['first_name'] = first_name
        gender_by_first_name[first_name].append(p['gender'])
    gender_by_first_name = {n: sum(v) / len(v) for n, v in gender_by_first_name.items()}

    for p in people:
        g = p['gender'] + gender_by_first_name[p['first_name']] * 0.3
        p['gender'] = 'female' if g < 0 else 'male'


def find_locations(people):
    alpha_3_to_2 = {c.alpha_3: c.alpha_2 for c in pycountry.countries}
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

    locations = set()
    for p in people:
        if p['locations']:
            locations.add(p['locations'][0])
    q = ["'%s'" % l.replace("'", "''") for l in locations if l]
    q = '(%s)' % ','.join(q)
    cursor.execute(
        "select wikipedia.title, ST_AsText(wikipedia.lng_lat) from wikipedia where title in " + q)
    loc_by_lat_lng = {
        t: None if p is None else wkt.loads(p)
        for t, p in cursor.fetchall()
    }
    locs_with_coos = [k for k in loc_by_lat_lng if loc_by_lat_lng[k]]
    coos_for_loc = [loc_by_lat_lng[k] for k in locs_with_coos]
    loc_df = gpd.GeoDataFrame([{'location': k} for k in locs_with_coos],
                              geometry=coos_for_loc, crs={'init': 'epsg:4326'})
    locs_with_countries = gpd.sjoin(world, loc_df, how='right')
    locs_with_countries.reset_index()
    locs_with_countries = {rec['location']: (rec['iso_a3'], rec['name'], rec['continent'])
                           for _, rec in locs_with_countries.iterrows()}

    missing = set()
    for p in people:
        lat_lng = None
        iso_a3 = ''
        country = ''
        continent = ''
        for loc in p['locations'] + p['wikilinks']:
            lat_lng = loc_by_lat_lng.get(loc)
            if lat_lng:
                iso_a3, country, continent = locs_with_countries.get(loc)
                if country == 'France':
                    iso_a3 = 'FRA'
                elif country == 'Norway':
                    iso_a3 = 'NOR'
                break
        p['lat_lng'] = lat_lng
        p['country_code'] = alpha_3_to_2.get(iso_a3, '')
        if not p['country_code']:
           missing.add(iso_a3)
        p['country'] = country
        p['continent'] = continent

    print(missing)


def add_fields(people):
    field_mapping = yaml.safe_load(open('field_mapping.yaml'))
    infobox_mapping = yaml.safe_load(open('infobox_mapping.yaml'))

    for p in people:
        field_count = Counter()
        for l in p['wikilinks']:
            if l.startswith(CAT_PREFIX):
                last_word = l[len(CAT_PREFIX):].lower().rsplit(' ', 1)[-1]
                f = field_mapping.get(last_word)
                if f:
                    field_count[f] += 1
        infobox_field = infobox_mapping.get(p['infobox'])
        if infobox_field:
            field_count[infobox_field] += 3

        if field_count:
            field = field_count.most_common(1)[0][0]
        else:
            field = ''
        p['field'] = field


def main(json_dir, cursor, min_year=1500, max_year=2000):
    people = fetch_people(json_dir, cursor, max_year, min_year)
    print('assigning genders')
    assign_genders(people)
    print('finding locations')
    find_locations(people)
    print('adding fields')
    add_fields(people)
    print('inserting data')
    seen = set()
    for p in people:
        if p['person_name'] in seen:
            continue
        seen.add(p['person_name'])
        cursor.execute("INSERT INTO wikitrends "
                       "(person_name, view_count, year_born, year_died, word_count, gender, "
                       "continent, country_code, field) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (p['person_name'], p['view_count'],
                        p['born'], p['died'],
                        p['word_count'], p['gender'],
                        p['continent'], p['country_code'], p['field']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import wikidata into postgress')
    parser.add_argument('--postgres', type=str, help='postgres connection string')
    parser.add_argument('json_dir', type=str, help='directory to store intermediate jsons')

    args = parser.parse_args()
    conn, cursor = setup_db(args.postgres)

    if not os.path.isdir(args.json_dir):
        os.makedirs(args.json_dir)

    main(args.json_dir, cursor)

    conn.commit()
