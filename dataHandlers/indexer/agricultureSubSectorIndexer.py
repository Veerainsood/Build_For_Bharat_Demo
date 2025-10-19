import duckdb, json
from pathlib import Path

DB_PATH = "dataHandlers/data/ogdp_index.db"
ROOT_DIR = Path("dataHandlers/data/sectors")

# define logical umbrellas
GROUPS = {
    # 1️⃣ scheme-level
    # "Beneficiaries (PM-KISAN)": ["PM-KISAN"],  # separate index already handled

    # 2️⃣ direct market API
    "Agricultural Marketing": ["Agricultural Marketing"],

    # 3️⃣ merged crop family
    "Crop Development & Seed Production": [
        "Crops",
        "Horticulture",
        "Seeds",
        "Agricultural Produces",
    ],

    # 4️⃣ merged ICAR/education/biotech
    "Research, Education & Biotechnology": [
        "Agricultural Research & Extension",
        "Education",
        "Biotechnology",
    ],
}

def safe_filename(name: str) -> str:
    return (
        name.lower()
        .replace(" & ", "_and_")
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
    )

def build_grouped_sector_files():
    con = duckdb.connect(DB_PATH, read_only=True)
    ROOT_DIR.mkdir(parents=True, exist_ok=True)

    for group_name, sectors in GROUPS.items():
        all_rows = []
        for s in sectors:
            rows = con.execute(
                "SELECT id, title FROM datasets WHERE lower(sector) LIKE ?",
                [f"%{s.lower()}%"],
            ).fetchall()
            all_rows.extend(rows)

        if not all_rows:
            print(f"⚠️  No datasets found for {group_name}")
            continue

        data = {group_name: [{"id": r[0], "title": r[1]} for r in all_rows]}
        path = ROOT_DIR / f"{safe_filename(group_name)}.json"
        path.write_text(json.dumps(data, indent=2))

        print(f"✅ {group_name}: {len(all_rows)} datasets → {path}")

    print("\n🎯 All grouped sector files written to:", ROOT_DIR)

def main():
    build_grouped_sector_files()

if __name__ == "__main__":
    build_grouped_sector_files()
