import re, json, duckdb
from collections import defaultdict
from pathlib import Path
from difflib import get_close_matches

DB_PATH = "dataHandlers/data/ogdp_index.db"
ROOT_DIR = Path("dataHandlers/data/sectors/Beneficiaries_(PM_KISAN)")
INDEX_PATH = Path("dataHandlers/data/sectors/Beneficiaries_(PM_KISAN).json")

# ---------------------- REGEX ----------------------
PMKISAN_REGEX = re.compile(
    r"""
    village\s+and\s+gender[-\s]?wise\s+beneficiaries?\s+count\s+of\s+
    (.+?)\s+                           # district (lazy)
    district\s+of\s+
    (.+?)\s+                           # state (lazy)
    under\s+the\s+pm-?kisan\s+scheme\s+for\s+
    (\d+)(?:st|nd|rd|th)\s+            # installment number
    instal?ment\s*                     # installment / instalment
    [\-\u2013]\s*                      # hyphen or en dash
    (\d{4})\s*[-/]\s*(\d{2,4})         # year: 2023-24 or 2023-2024
    """,
    re.IGNORECASE | re.VERBOSE,
)

# ---------------------- STATE UTILITIES ----------------------
STATE_LIST = [
    "Andhra Pradesh","Arunachal Pradesh","Assam","Bihar","Chhattisgarh","Goa","Gujarat","Haryana",
    "Himachal Pradesh","Jharkhand","Karnataka","Kerala","Madhya Pradesh","Maharashtra","Manipur",
    "Meghalaya","Mizoram","Nagaland","Odisha","Punjab","Rajasthan","Sikkim","Tamil Nadu","Telangana",
    "Tripura","Uttar Pradesh","Uttarakhand","West Bengal","Andaman & Nicobar Islands","Chandigarh",
    "Dadra and Nagar Haveli and Daman and Diu","Delhi","Jammu & Kashmir","Ladakh","Lakshadweep","Puducherry"
]

def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z]", "", s.lower())

def match_state(raw_state: str) -> str:
    """Map any raw state string to a canonical display name from STATE_LIST."""
    key = _norm_key(raw_state)
    cand_map = {_norm_key(s): s for s in STATE_LIST}
    if key in cand_map:
        return cand_map[key]
    close = get_close_matches(key, cand_map.keys(), n=1, cutoff=0.6)
    return cand_map[close[0]] if close else raw_state.strip()

def state_filename(canonical: str) -> str:
    name = canonical.strip().lower()
    name = re.sub(r"&", " and ", name)
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return f"{name}.json"

# ---------------------- MAIN INDEXER ----------------------
def build_agriculture_index():
    con = duckdb.connect(DB_PATH, read_only=True)
    rows = con.execute("SELECT id, title FROM datasets WHERE lower(title) LIKE '%pm-kisan%'").fetchall()

    agg = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    url_map = {}
    total = 0

    for url, title in rows:
        if not title:
            continue
        m = PMKISAN_REGEX.search(title)
        # breakpoint()
        if not m:
            continue
        raw_district, raw_state, inst_num, year_begin, year_end = m.groups()

        raw_district = raw_district.strip()
        raw_state = raw_state.strip()
        year_begin, year_end = year_begin.strip(), year_end.strip()
        year = f"{year_begin}-{year_end[-2:]}"  # normalize e.g. 2023-24
        inst = f"{inst_num}th"

        canonical_state = match_state(raw_state)
        agg[canonical_state][raw_district][year].add(inst)

        key = f"{raw_district}:{year}[{inst}]"
        url_map[key] = url
        total += 1

    ROOT_DIR.mkdir(parents=True, exist_ok=True)
    state_map = {}

    for canonical_state, districts in agg.items():
        entries = []
        url_entries = {}

        for raw_district, years in districts.items():
            for year, insts in years.items():
                def _inst_key(x: str) -> int:
                    m = re.match(r"(\d+)", x)
                    return int(m.group(1)) if m else 0

                inst_list = ",".join(sorted(insts, key=_inst_key))
                key = f"{raw_district}:{year}[{inst_list}]"
                entries.append(key)

                # map one merged key to list of URLs that contributed
                urls_for_key = [
                    u for k, u in url_map.items()
                    if k.startswith(f"{raw_district}:{year}[")  # match subset
                ]
                url_entries[key] = urls_for_key

        fname = state_filename(canonical_state)
        state_file = ROOT_DIR / fname
        url_file = ROOT_DIR / f"{fname.replace('.json', '_urls.json')}"

        state_file.write_text(json.dumps({"PM-KISAN": entries}, indent=2), encoding="utf-8")
        url_file.write_text(json.dumps({"PM-KISAN-Urls": url_entries}, indent=2), encoding="utf-8")

        state_map[canonical_state] = str(state_file)

    index_data = {
        "meta": {
            "template": "https://exlink.pmkisan.gov.in/services/GovDataDetails.asmx/GetPMKIsanDatagov",
            "fields": ["state", "district", "year", "installment"],
            "variants": total
        },
        "states": state_map
    }

    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(index_data, indent=2), encoding="utf-8")

    print(f"✅ Wrote {len(state_map)} state files with {total} total entries")
    print(f"✅ Root index: {INDEX_PATH}")

# ---------------------- ENTRYPOINT ----------------------
if __name__ == "__main__":
    build_agriculture_index()
