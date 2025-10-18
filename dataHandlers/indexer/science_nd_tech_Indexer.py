import duckdb, json, re
from collections import defaultdict
from pathlib import Path

DB_PATH = "dataHandlers/data/ogdp_index.db"
ROOT_DIR = Path("dataHandlers/data/sectors")
OUT_PATH = ROOT_DIR / "sector_index.json"

def safe_name(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", name.lower().replace(" ", "_"))

def build_science_subsectors():
    con = duckdb.connect(DB_PATH, read_only=True)
    rows = con.execute("""
        SELECT id, title, sector
        FROM datasets
        WHERE lower(sector) LIKE '%science%'
    """).fetchall()

    # buckets
    atmos_map = []
    earth_map = []

    for dataset_id, title, sector_str in rows:
        if not sector_str:
            continue
        for s in sector_str.split(","):
            s = s.strip()
            if s.lower() == "atmospheric science":
                atmos_map.append({"id": dataset_id, "title": title})
            elif s.lower() == "earth sciences":
                earth_map.append({"id": dataset_id, "title": title})

    ROOT_DIR.mkdir(parents=True, exist_ok=True)
    index = {}

    # write atmospheric science file
    if atmos_map:
        atmos_dir = ROOT_DIR / "atmospheric_science"
        atmos_dir.mkdir(exist_ok=True)
        atmos_file = atmos_dir / "atmospheric_science.json"
        atmos_file.write_text(json.dumps({"Atmospheric Science": atmos_map}, indent=2))
        index["Atmospheric Science"] = str(atmos_file)

    # write earth sciences file
    if earth_map:
        earth_dir = ROOT_DIR / "earth_sciences"
        earth_dir.mkdir(exist_ok=True)
        earth_file = earth_dir / "earth_sciences.json"
        earth_file.write_text(json.dumps({"Earth Sciences": earth_map}, indent=2))
        index["Earth Sciences"] = str(earth_file)

    OUT_PATH.write_text(json.dumps(index, indent=2))
    print(f"✅ Wrote {len(index)} science subsectors at {OUT_PATH}")

if __name__ == "__main__":
    build_science_subsectors()
