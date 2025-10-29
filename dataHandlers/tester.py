import pandas as pd
from pathlib import Path

def scan_crop_datasets(folder="dataHandlers/data/Research, Education & Biotechnology"):
    folder = Path(folder)
    bad_files = []
    # breakpoint()
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


# import pandas as pd
# from pathlib import Path
# import tempfile
# import shutil

# folder = Path("dataHandlers/data/Crop Development & Seed Production")
# encodings = ["utf-8", "utf-8-sig", "latin1", "windows-1252"]

# for f in folder.glob("*.csv"):
#     print(f"🔍 repairing {f.name}...")
#     for enc in encodings:
#         try:
#             df = pd.read_csv(f, encoding=enc)
#             # optional: quick cleanup
#             df = df.replace(["NA", "N.A.", "N/A", "--"], pd.NA)
#             df.columns = [str(c).strip() for c in df.columns]
#             # write to a temporary file first (safety)
#             with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
#                 df.to_csv(tmp.name, index=False)
#                 tmp_path = Path(tmp.name)
#             shutil.move(tmp_path, f)  # overwrite original
#             print(f"✅ repaired {f.name} with {enc}")
#             break
#         except Exception as e:
#             continue
#     else:
#         print(f"❌ could not repair {f.name}")



if __name__ == '__main__':
    scan_crop_datasets()