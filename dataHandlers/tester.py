import pandas as pd
from pathlib import Path

def scan_crop_datasets(folder="dataHandlers/data/Crop Development & Seed Production"):
    folder = Path(folder)
    bad_files = []

    for f in folder.glob("*.*"):
        if f.suffix.lower() not in [".csv", ".xls", ".xlsx"]:
            continue
        try:
            if f.suffix == ".csv":
                df = pd.read_csv(f)
            elif f.suffix == ".xls":
                df = pd.read_excel(f)
            else:
                df = pd.read_excel(f, engine="openpyxl")

            # basic sanity checks
            if df.empty:
                bad_files.append((f.name, "empty"))
            elif df.isna().all().all():
                bad_files.append((f.name, "all NaN"))
            elif len(df.columns) < 2:
                bad_files.append((f.name, "too few columns"))

        except Exception as e:
            bad_files.append((f.name, f"read error: {e}"))

    print("=== Damaged or suspicious files ===")
    for name, reason in bad_files:
        print(f"❌ {name} — {reason}")

    print(f"\n✅ Total files scanned: {len(list(folder.glob('*')))}")
    print(f"⚠️ Problematic files: {len(bad_files)}")
    return bad_files