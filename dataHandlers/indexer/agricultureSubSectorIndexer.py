import duckdb, json
from pathlib import Path
import pandas as pd
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

def safe_filename(name: str):
    return name.lower().replace("&", "and").replace(" ", "_").replace("/", "_")

def describe_file(file_path: Path, max_cols=6, max_rows=1, max_uniques=5):
    try:
        if file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path)
        elif file_path.suffix.lower() in [".xls", ".xlsx"]:
            df = pd.read_excel(file_path)
        elif file_path.suffix.lower() == ".json":
            df = pd.read_json(file_path)
        else:
            return None

        cols = list(df.columns)[:max_cols]
        desc = {
            "shape": df.shape,
            "columns": cols,
            "sample": df.head(max_rows).to_dict(orient="records")
        }

        uniques = {}
        for c in cols:
            if df[c].dtype == "object":
                vals = df[c].dropna().unique()[:max_uniques]
                uniques[c] = vals.tolist() if hasattr(vals, "tolist") else list(vals)
        if uniques:
            desc["unique_values"] = uniques

        return desc
    except Exception as e:
        return {"error": str(e)}


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

        group_dir = Path("dataHandlers/data") / group_name
        entries = []
        for idx, (id_, title) in enumerate(all_rows):
            entry = {"id": id_, "title": title, "index": idx}
            # attach descriptor if local file exists
            for ext in [".csv", ".xlsx", ".xls", ".json"]:
                file_path = group_dir / f"{idx}{ext}"
                if file_path.exists():
                    entry["describe"] = describe_file(file_path)
                    break
            entries.append(entry)

        data = {group_name: entries}
        out_path = ROOT_DIR / f"{safe_filename(group_name)}.json"
        out_path.write_text(json.dumps(data, indent=2))

        print(f"✅ {group_name}: {len(all_rows)} datasets → {out_path}")

    print("\n🎯 All grouped sector files written to:", ROOT_DIR)

def main():
    build_grouped_sector_files()

if __name__ == "__main__":
    build_grouped_sector_files()
