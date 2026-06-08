#!/usr/bin/env python3
"""Build wikipeople.db from Wikidata.

Queries query.wikidata.org for all humans (Q5) who have an English Wikipedia
article, batched by year of birth (1800-2000 by default). Resumable: years
already in import_progress are skipped unless --force is given.
"""

import argparse
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import unquote

import requests

ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"
USER_AGENT = "WikiPeopleImporter/1.0 (https://douwe.com; douwe.osinga@gmail.com)"
DEFAULT_DB = Path(__file__).parent / "static" / "wikipeople.db"

PREFIXES = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS person (
    qid TEXT PRIMARY KEY,
    enwiki_title TEXT NOT NULL,
    name TEXT,
    gender_qid TEXT,
    year_born INTEGER,
    year_died INTEGER,
    citizenship_country_codes TEXT,
    pob_qid TEXT,
    pob_lat REAL,
    pob_lon REAL,
    pob_country_code TEXT,
    pod_qid TEXT,
    pod_lat REAL,
    pod_lon REAL,
    pod_country_code TEXT,
    occupation_qids TEXT,
    field_qids TEXT,
    manner_of_death_qid TEXT,
    image_filename TEXT,
    sitelink_count INTEGER
);

CREATE INDEX IF NOT EXISTS idx_person_year_born ON person(year_born);
CREATE INDEX IF NOT EXISTS idx_person_pob_country ON person(pob_country_code);
CREATE INDEX IF NOT EXISTS idx_person_gender ON person(gender_qid);

CREATE TABLE IF NOT EXISTS import_progress (
    bucket TEXT PRIMARY KEY,
    imported_at TEXT NOT NULL,
    row_count INTEGER NOT NULL
);
"""

# One SPARQL query, parametrised by birth-year (and optionally birth-month for
# year-splits when a whole year times out). Multi-valued properties are
# collapsed with GROUP_CONCAT so we get one row per person.
QUERY_TMPL = PREFIXES + """
SELECT ?person ?article ?personLabel
       (SAMPLE(?dob_) AS ?dob)
       (SAMPLE(?dod_) AS ?dod)
       (SAMPLE(?gender_) AS ?gender)
       (GROUP_CONCAT(DISTINCT STR(?cit_code); separator="|") AS ?citCodes)
       (SAMPLE(?pob_) AS ?pob)
       (SAMPLE(?pobCoords_) AS ?pobCoords)
       (SAMPLE(?pobCC_) AS ?pobCC)
       (SAMPLE(?pod_) AS ?pod)
       (SAMPLE(?podCoords_) AS ?podCoords)
       (SAMPLE(?podCC_) AS ?podCC)
       (GROUP_CONCAT(DISTINCT STR(?occ_); separator="|") AS ?occupations)
       (GROUP_CONCAT(DISTINCT STR(?fld_); separator="|") AS ?fields)
       (SAMPLE(?manner_) AS ?manner)
       (SAMPLE(?image_) AS ?image)
       (SAMPLE(?sitelinks_) AS ?sitelinks)
WHERE {
  ?person wdt:P31 wd:Q5 ; wdt:P569 ?dob_ .
  ?article schema:about ?person ; schema:isPartOf <https://en.wikipedia.org/> .
  ?person wikibase:sitelinks ?sitelinks_ .
  FILTER(YEAR(?dob_) = %(year)d %(extra_filter)s)

  OPTIONAL { ?person rdfs:label ?personLabel . FILTER(LANG(?personLabel) = "en") }
  OPTIONAL { ?person wdt:P21 ?gender_ . }
  OPTIONAL { ?person wdt:P570 ?dod_ . }
  OPTIONAL { ?person wdt:P27 ?cit_ . ?cit_ wdt:P297 ?cit_code . }
  OPTIONAL { ?person wdt:P19 ?pob_ .
             OPTIONAL { ?pob_ wdt:P625 ?pobCoords_ . }
             OPTIONAL { ?pob_ wdt:P17 ?pobC_ . ?pobC_ wdt:P297 ?pobCC_ . } }
  OPTIONAL { ?person wdt:P20 ?pod_ .
             OPTIONAL { ?pod_ wdt:P625 ?podCoords_ . }
             OPTIONAL { ?pod_ wdt:P17 ?podC_ . ?podC_ wdt:P297 ?podCC_ . } }
  OPTIONAL { ?person wdt:P106 ?occ_ . }
  OPTIONAL { ?person wdt:P101 ?fld_ . }
  OPTIONAL { ?person wdt:P1196 ?manner_ . }
  OPTIONAL { ?person wdt:P18 ?image_ . }
}
GROUP BY ?person ?article ?personLabel
"""

POINT_RE = re.compile(r"Point\(([-0-9.]+)\s+([-0-9.]+)\)", re.IGNORECASE)


def parse_point(wkt):
    if not wkt:
        return None, None
    m = POINT_RE.match(wkt)
    if not m:
        return None, None
    return float(m.group(2)), float(m.group(1))  # lat, lon


def parse_year(iso):
    if not iso:
        return None
    sign = -1 if iso.startswith("-") else 1
    digits = iso.lstrip("-")
    try:
        return sign * int(digits[:4])
    except (ValueError, IndexError):
        return None


def qid_from_uri(uri):
    if not uri:
        return None
    return uri.rsplit("/", 1)[-1]


def qids_from_concat(blob):
    if not blob:
        return None
    qids = [qid_from_uri(u) for u in blob.split("|") if u]
    return "|".join(qids) or None


def title_from_url(url):
    if not url:
        return None
    return unquote(url.rsplit("/", 1)[-1]).replace("_", " ")


def row_for_db(b):
    def val(key):
        return b.get(key, {}).get("value")

    pob_lat, pob_lon = parse_point(val("pobCoords"))
    pod_lat, pod_lon = parse_point(val("podCoords"))
    sitelinks = val("sitelinks")
    return (
        qid_from_uri(val("person")),
        title_from_url(val("article")),
        val("personLabel"),
        qid_from_uri(val("gender")),
        parse_year(val("dob")),
        parse_year(val("dod")),
        val("citCodes") or None,
        qid_from_uri(val("pob")),
        pob_lat, pob_lon,
        val("pobCC"),
        qid_from_uri(val("pod")),
        pod_lat, pod_lon,
        val("podCC"),
        qids_from_concat(val("occupations")),
        qids_from_concat(val("fields")),
        qid_from_uri(val("manner")),
        val("image"),
        int(sitelinks) if sitelinks else None,
    )


def fetch(query, timeout):
    for attempt in range(4):
        r = requests.get(
            ENDPOINT,
            params={"query": query, "format": "json"},
            headers={"User-Agent": USER_AGENT,
                     "Accept": "application/sparql-results+json"},
            timeout=timeout,
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  rate-limited; sleeping {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"  {r.status_code}; retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()["results"]["bindings"]
    raise RuntimeError("repeated SPARQL failures")


def build_query(year, month=None):
    extra = f"&& MONTH(?dob_) = {month}" if month else ""
    return QUERY_TMPL % {"year": year, "extra_filter": extra}


INSERT_SQL = """
INSERT OR REPLACE INTO person (
    qid, enwiki_title, name, gender_qid,
    year_born, year_died, citizenship_country_codes,
    pob_qid, pob_lat, pob_lon, pob_country_code,
    pod_qid, pod_lat, pod_lon, pod_country_code,
    occupation_qids, field_qids, manner_of_death_qid,
    image_filename, sitelink_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def import_bucket(conn, bucket, query, timeout):
    bindings = fetch(query, timeout)
    rows = [row_for_db(b) for b in bindings]
    conn.executemany(INSERT_SQL, rows)
    conn.execute(
        "INSERT OR REPLACE INTO import_progress (bucket, imported_at, row_count) "
        "VALUES (?, datetime('now'), ?)",
        (bucket, len(rows)),
    )
    conn.commit()
    return len(rows)


def import_year(conn, year, timeout):
    bucket = str(year)
    try:
        n = import_bucket(conn, bucket, build_query(year), timeout)
        print(f"{year}: {n} rows")
        return
    except (requests.Timeout, RuntimeError) as e:
        print(f"{year}: {e} — splitting by month", file=sys.stderr)
    for month in range(1, 13):
        sub = f"{year}-{month:02d}"
        try:
            n = import_bucket(conn, sub, build_query(year, month), timeout)
            print(f"  {sub}: {n} rows")
        except (requests.Timeout, RuntimeError) as e:
            print(f"  {sub}: {e} — giving up", file=sys.stderr)
        time.sleep(1.0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=1800)
    parser.add_argument("--end", type=int, default=2000)
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--throttle", type=float, default=1.5,
                        help="seconds to wait between requests")
    parser.add_argument("--timeout", type=int, default=120,
                        help="HTTP timeout per request, seconds")
    parser.add_argument("--force", action="store_true",
                        help="re-import buckets already recorded")
    parser.add_argument("--only", type=int,
                        help="import just this one year (handy for testing)")
    args = parser.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)

    done = set()
    if not args.force:
        done = {r[0] for r in conn.execute("SELECT bucket FROM import_progress")}

    years = [args.only] if args.only else range(args.start, args.end + 1)
    for year in years:
        if str(year) in done:
            continue
        import_year(conn, year, args.timeout)
        time.sleep(args.throttle)

    conn.close()


if __name__ == "__main__":
    main()
