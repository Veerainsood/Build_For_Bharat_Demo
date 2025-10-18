import re, json, duckdb
from collections import defaultdict
from pathlib import Path
from difflib import get_close_matches

DB_PATH   = "dataHandlers/data/ogdp_index.db"
ROOT_DIR  = Path("dataHandlers/data/sectors/agriculture")
INDEX_PATH = Path("dataHandlers/data/sectors/agriculture_index.json")

# Matches: ... of {district} district of {state} under ... for {N}th instalment - {YYYY-YYYY}
PMKISAN_REGEX = re.compile(
    r"village and gender[- ]wise beneficiaries count of (.+?) district of (.+?) under the pm-kisan scheme for (\d+)(?:st|nd|rd|th) instalment - (\d{4}-\d{4})",
    re.IGNORECASE
)

# Canonical list of Indian States/UTs (display names)
STATE_LIST = [
    "Andhra Pradesh","Arunachal Pradesh","Assam","Bihar","Chhattisgarh","Goa","Gujarat","Haryana",
    "Himachal Pradesh","Jharkhand","Karnataka","Kerala","Madhya Pradesh","Maharashtra","Manipur",
    "Meghalaya","Mizoram","Nagaland","Odisha","Punjab","Rajasthan","Sikkim","Tamil Nadu","Telangana",
    "Tripura","Uttar Pradesh","Uttarakhand","West Bengal","Andaman & Nicobar Islands","Chandigarh",
    "Dadra and Nagar Haveli and Daman and Diu","Delhi","Jammu & Kashmir","Ladakh","Lakshadweep","Puducherry"
]

def _norm_key(s: str) -> str:
    # letters only, lowercase
    return re.sub(r"[^a-z]", "", s.lower())

def match_state(raw_state: str) -> str:
    """Map any raw state string to a canonical display name from STATE_LIST."""
    key = _norm_key(raw_state)
    cand_map = { _norm_key(s): s for s in STATE_LIST }
    if key in cand_map:
        return cand_map[key]
    # fuzzy fallback
    close = get_close_matches(key, cand_map.keys(), n=1, cutoff=0.6)
    return cand_map[close[0]] if close else raw_state.strip()

def state_filename(canonical: str) -> str:
    """
    Produce a single stable filename per canonical state:
    lowercased, spaces -> underscores, strip non-alnum/underscore.
    'Chandigarh' -> 'chandigarh.json'
    'Andaman & Nicobar Islands' -> 'andaman_nicobar_islands.json'
    """
    name = canonical.strip().lower()
    name = re.sub(r"&", " and ", name)
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return f"{name}.json"

def build_agriculture_index():
    con = duckdb.connect(DB_PATH, read_only=True)
    rows = con.execute(
        "SELECT title FROM datasets WHERE lower(title) LIKE '%pm-kisan%'"
    ).fetchall()

    # Aggregate as: canonical_state -> raw_district -> year -> set(installments)
    agg = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    total = 0

    for (title,) in rows:
        m = PMKISAN_REGEX.search(title or "")
        if not m:
            continue
        raw_district, raw_state, inst_num, year = m.groups()

        # preserve RAW strings (no title-casing) because API expects exact tokens
        raw_state = (raw_state or "").strip()
        raw_district = (raw_district or "").strip()
        year = (year or "").strip()
        inst = f"{inst_num}th"  # keep as-is (e.g., '2th' if that’s in the source)

        canonical_state = match_state(raw_state)
        agg[canonical_state][raw_district][year].add(inst)
        total += 1

    ROOT_DIR.mkdir(parents=True, exist_ok=True)

    # Write one file per canonical state; entries preserve RAW district strings
    state_map = {}  # for the tiny root index (canonical display -> path)
    for canonical_state, districts in agg.items():
        entries = []
        for raw_district, years in districts.items():
            for year, insts in years.items():
                # sort installments numerically (strip 'th'/'st' suffix safely)
                def _inst_key(x: str) -> int:
                    m = re.match(r"(\d+)", x)
                    return int(m.group(1)) if m else 0
                inst_list = ",".join(sorted(insts, key=_inst_key))
                # IMPORTANT: keep the raw district spelling exactly
                entries.append(f"{raw_district}:{year}[{inst_list}]")

        # stable, canonical filename per state
        fname = state_filename(canonical_state)
        state_file = ROOT_DIR / fname
        state_file.write_text(json.dumps({"PM-KISAN": entries}, indent=2), encoding="utf-8")

        # root index shows canonical display name -> path
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

if __name__ == "__main__":
    build_agriculture_index()
