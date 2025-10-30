import duckdb, json, re
from collections import defaultdict
from pathlib import Path
import pandas as pd
DB_PATH = "dataHandlers/data/ogdp_index.db"
ROOT_DIR = Path("dataHandlers/data/sectors")
OUT_PATH = ROOT_DIR / "sector_index.json"

EXTRA_EARTH_DATASETS = [
    {
        "id": "https://data.gov.in/files/ogd20/ogdpv2dms/s3fs-public/datafile/District_Rainfall_Normal_0.xls",
        "title": "District Rainfall Normal (in mm) Monthly, Seasonal And Annual : Data Period 1951-2000",
        "index": 0
    },
    {
        "id": "https://data.gov.in/files/ogd20/ogdpv2dms/s3fs-public/datafile/Area_Weighted_Monthly_Seasonal_And_Annual_Rainfall_0.xls",
        "title": "Area Weighted Monthly, Seasonal And Annual Rainfall (in mm) For 36 Meteorological Subdivisions",
        "index": 1
    },
    {
        "id": "https://data.gov.in/files/ogd20/ogdpv2dms/s3fs-public/datafile/All_India_Area_Weighted_Monthly_Seasonal_And_Annual_Rainfall.xls",
        "title": "All India Area Weighted Monthly, Seasonal And Annual Rainfall (in mm)",
        "index": 2
    },
    {
        "id": "https://data.gov.in/files/ogd20/ogdpv2dms/s3fs-public/datafile/DEP_STORM_DATA_1.csv",
        "title": "Number of Depressions/Deep Depressions Formed Over the North Indian Ocean",
        "index": 3
    },
    {
        "id": "https://data.gov.in/files/ogd20/ogdpv2dms/s3fs-public/datafile/CS_STORM_DATA_1.csv",
        "title": "Number of Cyclonic Storms/Severe Cyclonic Storms formed over the North Indian Ocean",
        "index": 4
    },
    {
        "id": "https://data.gov.in/files/ogd20/ogdpv2dms/s3fs-public/datafile/Mean_Temperatures_India1901-2012.csv",
        "title": "Annual And Seasonal Mean Temperature Of India",
        "index": 5
    },
    {
        "id": "https://data.gov.in/files/ogd20/ogdpv2dms/s3fs-public/datafile/India_Max_Temperatures_1901-2012_1.xls",
        "title": "Annual And Seasonal Maximum Temperature Of India",
        "index": 6
    },
    {
        "id": "https://data.gov.in/files/ogd20/ogdpv2dms/s3fs-public/datafile/India_Min_Temperatures_1901-2012_1.xls",
        "title": "Annual And Seasonal Minimum Temperature Of India",
        "index": 7
    },
]

def safe_name(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", name.lower().replace(" ", "_"))

# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
def build_science_subsectors():
    con = duckdb.connect(DB_PATH, read_only=True)
    rows = con.execute("""
        SELECT id, title, sector
        FROM datasets
        WHERE lower(sector) LIKE '%science%'
    """).fetchall()

    earth_map = []
    earth_map.extend(EXTRA_EARTH_DATASETS)
    idx = len(EXTRA_EARTH_DATASETS)
    for dataset_id, title, sector_str in rows:
        if sector_str and "earth sciences" in sector_str.lower():
            earth_map.append({"id": dataset_id, "title": title, "index": idx})
            idx += 1

    # deduplicate by id
    seen, deduped = set(), []
    for e in earth_map:
        if e["id"] not in seen:
            deduped.append(e)
            seen.add(e["id"])

    # enrich each entry with descriptor if local file exists
    family_name = "Temperature and Rainfall"
    data_dir = Path("dataHandlers/data") / family_name
    for entry in deduped:
        for ext in [".csv", ".xls", ".xlsx", ".json"]:
            f = data_dir / f"{entry['index']}{ext}"
            if f.exists():
                entry["describe"] = describe_file(f)
                break

    ROOT_DIR.mkdir(parents=True, exist_ok=True)
    earth_file = ROOT_DIR / "temperature_and_rainfall.json"
    earth_file.write_text(json.dumps({family_name: deduped}, indent=2))

    OUT_PATH.write_text(json.dumps({family_name: str(earth_file)}, indent=2))
    print(f"✅ Wrote {len(deduped)} total Earth+Atmosphere datasets to {earth_file}")

# -------------------------------------------------------------------
def main():
    build_science_subsectors()

if __name__ == "__main__":
    build_science_subsectors()