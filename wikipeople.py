import json
import math
import sqlite3
from collections import Counter
from pathlib import Path

from projects.common import HttpResponse, HttpResponseBadRequest, Project

DB_PATH = Path(__file__).parent / "static" / "wikipeople.db"

# Cap to where data is dense and clean. The DB contains 1000-2025 but pre-1500
# is thin and post-2010 falls off a cliff (kids don't have wiki articles).
YEAR_MIN, YEAR_MAX = 1500, 2010

# Wikidata QIDs for the two gender values that account for >99.9% of people.
GENDER_MALE = "Q6581097"
GENDER_FEMALE = "Q6581072"

# Minimum sample size before a country shows on the gender map — otherwise
# single-figure birthplaces (e.g. Vatican) dominate the colour scale.
MIN_POB_COUNT = 50

# ISO 3166-1 alpha-2 → alpha-3 (Plotly choropleth wants alpha-3 with 'ISO-3').
ISO_A2_TO_A3 = {
    'AD': 'AND', 'AE': 'ARE', 'AF': 'AFG', 'AG': 'ATG', 'AI': 'AIA', 'AL': 'ALB',
    'AM': 'ARM', 'AO': 'AGO', 'AQ': 'ATA', 'AR': 'ARG', 'AS': 'ASM', 'AT': 'AUT',
    'AU': 'AUS', 'AW': 'ABW', 'AX': 'ALA', 'AZ': 'AZE', 'BA': 'BIH', 'BB': 'BRB',
    'BD': 'BGD', 'BE': 'BEL', 'BF': 'BFA', 'BG': 'BGR', 'BH': 'BHR', 'BI': 'BDI',
    'BJ': 'BEN', 'BL': 'BLM', 'BM': 'BMU', 'BN': 'BRN', 'BO': 'BOL', 'BQ': 'BES',
    'BR': 'BRA', 'BS': 'BHS', 'BT': 'BTN', 'BV': 'BVT', 'BW': 'BWA', 'BY': 'BLR',
    'BZ': 'BLZ', 'CA': 'CAN', 'CC': 'CCK', 'CD': 'COD', 'CF': 'CAF', 'CG': 'COG',
    'CH': 'CHE', 'CI': 'CIV', 'CK': 'COK', 'CL': 'CHL', 'CM': 'CMR', 'CN': 'CHN',
    'CO': 'COL', 'CR': 'CRI', 'CU': 'CUB', 'CV': 'CPV', 'CW': 'CUW', 'CX': 'CXR',
    'CY': 'CYP', 'CZ': 'CZE', 'DE': 'DEU', 'DJ': 'DJI', 'DK': 'DNK', 'DM': 'DMA',
    'DO': 'DOM', 'DZ': 'DZA', 'EC': 'ECU', 'EE': 'EST', 'EG': 'EGY', 'EH': 'ESH',
    'ER': 'ERI', 'ES': 'ESP', 'ET': 'ETH', 'FI': 'FIN', 'FJ': 'FJI', 'FK': 'FLK',
    'FM': 'FSM', 'FO': 'FRO', 'FR': 'FRA', 'GA': 'GAB', 'GB': 'GBR', 'GD': 'GRD',
    'GE': 'GEO', 'GF': 'GUF', 'GG': 'GGY', 'GH': 'GHA', 'GI': 'GIB', 'GL': 'GRL',
    'GM': 'GMB', 'GN': 'GIN', 'GP': 'GLP', 'GQ': 'GNQ', 'GR': 'GRC', 'GS': 'SGS',
    'GT': 'GTM', 'GU': 'GUM', 'GW': 'GNB', 'GY': 'GUY', 'HK': 'HKG', 'HM': 'HMD',
    'HN': 'HND', 'HR': 'HRV', 'HT': 'HTI', 'HU': 'HUN', 'ID': 'IDN', 'IE': 'IRL',
    'IL': 'ISR', 'IM': 'IMN', 'IN': 'IND', 'IO': 'IOT', 'IQ': 'IRQ', 'IR': 'IRN',
    'IS': 'ISL', 'IT': 'ITA', 'JE': 'JEY', 'JM': 'JAM', 'JO': 'JOR', 'JP': 'JPN',
    'KE': 'KEN', 'KG': 'KGZ', 'KH': 'KHM', 'KI': 'KIR', 'KM': 'COM', 'KN': 'KNA',
    'KP': 'PRK', 'KR': 'KOR', 'KW': 'KWT', 'KY': 'CYM', 'KZ': 'KAZ', 'LA': 'LAO',
    'LB': 'LBN', 'LC': 'LCA', 'LI': 'LIE', 'LK': 'LKA', 'LR': 'LBR', 'LS': 'LSO',
    'LT': 'LTU', 'LU': 'LUX', 'LV': 'LVA', 'LY': 'LBY', 'MA': 'MAR', 'MC': 'MCO',
    'MD': 'MDA', 'ME': 'MNE', 'MF': 'MAF', 'MG': 'MDG', 'MH': 'MHL', 'MK': 'MKD',
    'ML': 'MLI', 'MM': 'MMR', 'MN': 'MNG', 'MO': 'MAC', 'MP': 'MNP', 'MQ': 'MTQ',
    'MR': 'MRT', 'MS': 'MSR', 'MT': 'MLT', 'MU': 'MUS', 'MV': 'MDV', 'MW': 'MWI',
    'MX': 'MEX', 'MY': 'MYS', 'MZ': 'MOZ', 'NA': 'NAM', 'NC': 'NCL', 'NE': 'NER',
    'NF': 'NFK', 'NG': 'NGA', 'NI': 'NIC', 'NL': 'NLD', 'NO': 'NOR', 'NP': 'NPL',
    'NR': 'NRU', 'NU': 'NIU', 'NZ': 'NZL', 'OM': 'OMN', 'PA': 'PAN', 'PE': 'PER',
    'PF': 'PYF', 'PG': 'PNG', 'PH': 'PHL', 'PK': 'PAK', 'PL': 'POL', 'PM': 'SPM',
    'PN': 'PCN', 'PR': 'PRI', 'PS': 'PSE', 'PT': 'PRT', 'PW': 'PLW', 'PY': 'PRY',
    'QA': 'QAT', 'RE': 'REU', 'RO': 'ROU', 'RS': 'SRB', 'RU': 'RUS', 'RW': 'RWA',
    'SA': 'SAU', 'SB': 'SLB', 'SC': 'SYC', 'SD': 'SDN', 'SE': 'SWE', 'SG': 'SGP',
    'SH': 'SHN', 'SI': 'SVN', 'SJ': 'SJM', 'SK': 'SVK', 'SL': 'SLE', 'SM': 'SMR',
    'SN': 'SEN', 'SO': 'SOM', 'SR': 'SUR', 'SS': 'SSD', 'ST': 'STP', 'SV': 'SLV',
    'SX': 'SXM', 'SY': 'SYR', 'SZ': 'SWZ', 'TC': 'TCA', 'TD': 'TCD', 'TF': 'ATF',
    'TG': 'TGO', 'TH': 'THA', 'TJ': 'TJK', 'TK': 'TKL', 'TL': 'TLS', 'TM': 'TKM',
    'TN': 'TUN', 'TO': 'TON', 'TR': 'TUR', 'TT': 'TTO', 'TV': 'TUV', 'TW': 'TWN',
    'TZ': 'TZA', 'UA': 'UKR', 'UG': 'UGA', 'UM': 'UMI', 'US': 'USA', 'UY': 'URY',
    'UZ': 'UZB', 'VA': 'VAT', 'VC': 'VCT', 'VE': 'VEN', 'VG': 'VGB', 'VI': 'VIR',
    'VN': 'VNM', 'VU': 'VUT', 'WF': 'WLF', 'WS': 'WSM', 'YE': 'YEM', 'YT': 'MYT',
    'ZA': 'ZAF', 'ZM': 'ZMB', 'ZW': 'ZWE',
}


def _map_layout():
    return {
        'geo': {
            'projection': {'type': 'natural earth'},
            'showcoastlines': True, 'coastlinecolor': '#888',
            'showland': True, 'landcolor': '#eee',
            'showocean': False,
            'bgcolor': 'rgba(0,0,0,0)',
            'showframe': False,
        },
        'margin': {'t': 0, 'b': 0, 'l': 0, 'r': 0},
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'autosize': True, 'height': 440,
    }


def _stacked_layout(ylabel):
    return {
        'xaxis': {'title': {'text': 'Year born'}, 'tickformat': 'd'},
        'yaxis': {'title': {'text': ylabel}, 'ticksuffix': '%', 'range': [0, 100]},
        'autosize': True, 'height': 440,
        'margin': {'t': 20, 'b': 80, 'l': 60, 'r': 20},
        'hovermode': 'x unified',
        'legend': {'orientation': 'h', 'y': -0.18},
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': '#fafafa',
    }


def _gender_map(conn):
    rows = conn.execute(f"""
        SELECT pob_country_code,
               sum(gender_qid = '{GENDER_MALE}')   AS male,
               sum(gender_qid = '{GENDER_FEMALE}') AS female
        FROM person
        WHERE pob_country_code IS NOT NULL
          AND year_born BETWEEN {YEAR_MIN} AND {YEAR_MAX}
          AND gender_qid IN ('{GENDER_MALE}', '{GENDER_FEMALE}')
        GROUP BY pob_country_code
        HAVING (male + female) >= {MIN_POB_COUNT}
    """).fetchall()

    locations, scores, totals = [], [], []
    for cc, male, female in rows:
        a3 = ISO_A2_TO_A3.get(cc)
        if not a3:
            continue
        total = male + female
        locations.append(a3)
        scores.append(round(female / total, 4))
        totals.append(total)

    traces = [{
        'type': 'choropleth',
        'locationmode': 'ISO-3',
        'locations': locations,
        'z': scores,
        'text': totals,
        'hovertemplate':
            'Female share: %{z:.1%}<br>People: %{text:,}<extra>%{location}</extra>',
        'colorscale': [
            [0.0, '#313695'], [0.25, '#74add1'], [0.5, '#ffffbf'],
            [0.75, '#fdae61'], [1.0, '#a50026'],
        ],
        'zmin': 0, 'zmax': 0.35,
        'showscale': True,
        'colorbar': {'thickness': 12, 'len': 0.7, 'outlinewidth': 0,
                     'tickformat': '.0%'},
        'marker': {'line': {'color': '#888', 'width': 0.4}},
    }]
    return traces, _map_layout()


def _density_map(conn):
    rows = conn.execute(f"""
        SELECT pob_country_code, count(*) AS n
        FROM person
        WHERE pob_country_code IS NOT NULL
          AND year_born BETWEEN {YEAR_MIN} AND {YEAR_MAX}
        GROUP BY pob_country_code
    """).fetchall()

    locations, scores, totals = [], [], []
    for cc, n in rows:
        a3 = ISO_A2_TO_A3.get(cc)
        if not a3:
            continue
        locations.append(a3)
        scores.append(round(math.log10(n), 3))
        totals.append(n)

    traces = [{
        'type': 'choropleth',
        'locationmode': 'ISO-3',
        'locations': locations,
        'z': scores,
        'text': totals,
        'hovertemplate': 'People: %{text:,}<extra>%{location}</extra>',
        'colorscale': 'Viridis',
        'showscale': True,
        'colorbar': {'thickness': 12, 'len': 0.7, 'outlinewidth': 0,
                     'title': {'text': 'log₁₀(people)'}},
        'marker': {'line': {'color': '#888', 'width': 0.4}},
    }]
    return traces, _map_layout()


def _gender_timeline(conn):
    rows = conn.execute(f"""
        SELECT year_born,
               sum(gender_qid = '{GENDER_MALE}')   AS male,
               sum(gender_qid = '{GENDER_FEMALE}') AS female,
               sum(gender_qid NOT IN ('{GENDER_MALE}', '{GENDER_FEMALE}'))
                                                   AS other
        FROM person
        WHERE year_born BETWEEN {YEAR_MIN} AND {YEAR_MAX}
          AND gender_qid IS NOT NULL
        GROUP BY year_born
        ORDER BY year_born
    """).fetchall()

    years = [r[0] for r in rows]
    traces = []
    for idx, name in enumerate(('male', 'female', 'other')):
        y = [r[idx + 1] for r in rows]
        traces.append({
            'type': 'scatter', 'mode': 'lines', 'name': name,
            'x': years, 'y': y,
            'stackgroup': 'one', 'groupnorm': 'percent',
            'hovertemplate': f'{name}: %{{y:.1f}}%<extra>%{{x}}</extra>',
        })
    return traces, _stacked_layout('Share of people born')


def _occupation_timeline(conn, top_n=10):
    """Stacked area of the top-N occupations' share per birth year.

    A person can have multiple occupations, so the per-year denominator is the
    sum of (top-N occupation slots filled), not the headcount. The chart still
    reads as 'composition of recorded occupations', which is the intended story.
    """
    counter = Counter()
    for (blob,) in conn.execute(
            "SELECT occupation_qids FROM person WHERE occupation_qids IS NOT NULL"):
        for q in blob.split('|'):
            counter[q] += 1
    top_qids = [q for q, _ in counter.most_common(top_n)]
    top_idx = {q: i for i, q in enumerate(top_qids)}

    labels = dict(conn.execute(
        f"SELECT qid, label FROM qid_label WHERE qid IN "
        f"({','.join(['?'] * len(top_qids))})", top_qids))

    counts = {}
    rows = conn.execute(f"""
        SELECT year_born, occupation_qids FROM person
        WHERE occupation_qids IS NOT NULL
          AND year_born BETWEEN {YEAR_MIN} AND {YEAR_MAX}
    """)
    for year, blob in rows:
        slots = counts.setdefault(year, [0] * top_n)
        for q in blob.split('|'):
            i = top_idx.get(q)
            if i is not None:
                slots[i] += 1

    years = sorted(counts)
    traces = []
    for i, qid in enumerate(top_qids):
        name = labels.get(qid, qid)
        y = [counts[year][i] for year in years]
        traces.append({
            'type': 'scatter', 'mode': 'lines', 'name': name,
            'x': years, 'y': y,
            'stackgroup': 'one', 'groupnorm': 'percent',
            'hovertemplate': f'{name}: %{{y:.1f}}%<extra>%{{x}}</extra>',
        })
    return traces, _stacked_layout('Share of top-10 occupations')


BUILDERS = {
    ('map', 'gender'): _gender_map,
    ('map', 'density'): _density_map,
    ('time_line', 'gender'): _gender_timeline,
    ('time_line', 'occupation'): _occupation_timeline,
}

# (graph_type, graph_what) → JSON blob. Computed once per process; cleared on
# restart. The underlying DB only changes when build_db.py is re-run, so cache
# invalidation isn't a concern.
_CHART_CACHE = {}


def _chart_json(graph_type, graph_what):
    key = (graph_type, graph_what)
    if key not in _CHART_CACHE:
        with sqlite3.connect(DB_PATH) as conn:
            traces, layout = BUILDERS[key](conn)
        _CHART_CACHE[key] = json.dumps({'traces': traces, 'layout': layout})
    return _CHART_CACHE[key]


class WikiPeople(Project):
    def handle_request(self, handler, request):
        if handler != 'data':
            return None
        key = (request.GET.get('graph_type'), request.GET.get('graph_what'))
        if key not in BUILDERS:
            return HttpResponseBadRequest('unknown chart')
        return HttpResponse(_chart_json(*key), content_type='application/json')
