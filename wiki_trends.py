import argparse
import json
import os

import psycopg2
import psycopg2.extras
import mwparserfromhell
import re
from collections import Counter, defaultdict

WORD_RE = re.compile(r'\w+')
CAT_PREFIX = 'Category:'
DIED_POSTFIX = ' deaths'
BIRTH_POSTFIX = ' births'


def setup_db(connection_string):
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute('DROP TABLE IF EXISTS wikitrends')
    cursor.execute('CREATE TABLE wikitrends ('
                   '    title TEXT PRIMARY KEY,'
                   '    viewcount INTEGER,'
                   '    wordcount INTEGER,'
                   '    year_born INTEGER,'
                   '    year_died INTEGER, '
                   '    is_female BOOLEAN'
                   ')')
    cursor.execute('CREATE INDEX wikitrends_viewcount ON wikitrends(viewcount)')
    cursor.execute('CREATE INDEX wikitrends_wordcount ON wikitrends(wordcount)')
    cursor.execute('CREATE INDEX wikitrends_year_born ON wikitrends(year_born)')
    cursor.execute('CREATE INDEX wikitrends_is_female ON wikitrends(is_female)')

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
    wordcount = len(words)
    wordcounts = Counter(words)
    gender_words = {w: wordcounts[w] for w in ('him', 'his', 'he', 'her', 'she')}

    res = {}
    for template in parsed.filter_templates():
        if template.name.lower().startswith('infobox '):
            for param in template.params:
                res[param.name.strip().lower()] = param.value
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

    return {'name': rec['title'],
            'locations': locations,
            'word_count': wordcount,
            'gender_words': gender_words,
            'viewcount': rec['viewcount'],
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
        first_name = p['name'].split(' ', 1)[0]
        p['first_name'] = first_name
        gender_by_first_name[first_name].append(p['gender'])
    gender_by_first_name = {n: sum(v) / len(v) for n, v in gender_by_first_name.items()}

    for p in people:
        g = p['gender'] + gender_by_first_name[p['first_name']] * 0.3
        p['is_female'] = g < 0


def main(json_dir, cursor, min_year=1800, max_year=2000):
    people = fetch_people(json_dir, cursor, max_year, min_year)
    print('assigning genders')
    assign_genders(people)
    print('inserting data')
    seen = set()
    for p in people:
        if p['name'] in seen:
            continue
        seen.add(p['name'])
        cursor.execute("INSERT INTO wikitrends "
                       "(title, viewcount, year_born, year_died, wordcount, is_female) "
                       "VALUES (%s, %s, %s, %s, %s, %s)",
                       (p['name'], p['viewcount'],
                        p['born'], p['died'],
                        p['word_count'], p['is_female']))


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
