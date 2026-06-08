#!/usr/bin/env python3
"""Populate qid_label in wikipeople.db.

Collects every QID referenced in the person table (gender, occupation, field,
manner-of-death, place-of-birth, place-of-death) and asks QLever for its
English label. Resumable: only QIDs without a label are queried.
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

import requests

ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"
USER_AGENT = "WikiPeopleImporter/1.0 (https://douwe.com; douwe.osinga@gmail.com)"
DEFAULT_DB = Path(__file__).parent / "static" / "wikipeople.db"
BATCH_SIZE = 1000

SCHEMA = """
CREATE TABLE IF NOT EXISTS qid_label (
    qid TEXT PRIMARY KEY,
    label TEXT NOT NULL
);
"""

QUERY_TMPL = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?qid ?label WHERE {
  VALUES ?qid { %s }
  ?qid rdfs:label ?label .
  FILTER(LANG(?label) = "en")
}
"""


def collect_qids(conn):
    qids = set()
    cur = conn.execute("""
        SELECT gender_qid, manner_of_death_qid, pob_qid, pod_qid,
               occupation_qids, field_qids
        FROM person
    """)
    for gender, manner, pob, pod, occs, flds in cur:
        for q in (gender, manner, pob, pod):
            if q:
                qids.add(q)
        for blob in (occs, flds):
            if blob:
                qids.update(blob.split("|"))
    return qids


def fetch_labels(qids):
    values = " ".join(f"wd:{q}" for q in qids)
    query = QUERY_TMPL % values
    for attempt in range(4):
        r = requests.post(
            ENDPOINT,
            data={"query": query},
            headers={"User-Agent": USER_AGENT,
                     "Accept": "application/sparql-results+json"},
            timeout=180,
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
        out = {}
        for b in r.json()["results"]["bindings"]:
            qid = b["qid"]["value"].rsplit("/", 1)[-1]
            out[qid] = b["label"]["value"]
        return out
    raise RuntimeError("repeated SPARQL failures")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--throttle", type=float, default=0.3)
    parser.add_argument("--force", action="store_true",
                        help="re-fetch even QIDs already labelled")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.executescript(SCHEMA)

    print("Collecting QIDs from person table ...", flush=True)
    needed = collect_qids(conn)
    print(f"  {len(needed)} distinct QIDs referenced", flush=True)

    if not args.force:
        have = {r[0] for r in conn.execute("SELECT qid FROM qid_label")}
        needed -= have
        print(f"  {len(needed)} still need a label", flush=True)

    needed = sorted(needed)
    for i in range(0, len(needed), BATCH_SIZE):
        chunk = needed[i:i + BATCH_SIZE]
        try:
            labels = fetch_labels(chunk)
        except RuntimeError as e:
            print(f"  batch {i}: {e} — skipping", file=sys.stderr)
            continue
        conn.executemany(
            "INSERT OR REPLACE INTO qid_label (qid, label) VALUES (?, ?)",
            labels.items(),
        )
        conn.commit()
        print(f"  {i + len(chunk)} / {len(needed)}  (+{len(labels)} labels)",
              flush=True)
        time.sleep(args.throttle)

    print("done.")
    conn.close()


if __name__ == "__main__":
    main()
