import duckdb, json
from collections import defaultdict
from pathlib import Path

DB_PATH = "dataHandlers/data/ogdp_index.db"
OUT_PATH = Path("dataHandlers/data/sector_index.json")

def build_sector_index():
    con = duckdb.connect(DB_PATH, read_only=True)
    rows = con.execute("SELECT id, title, sector FROM datasets").fetchall()

    sector_map = defaultdict(list)
    for dataset_id, title, sector_str in rows:
        if not sector_str:
            continue
        for s in sector_str.split(","):
            s = s.strip()
            if not s:
                continue
            sector_map[s].append({
                "id": dataset_id,
                "title": title
            })

    OUT_PATH.write_text(json.dumps(sector_map, indent=2))
    print(f"✅ Built sector index with {len(sector_map)} sectors at {OUT_PATH}")

if __name__ == "__main__":
    build_sector_index()


