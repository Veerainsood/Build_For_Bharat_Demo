# dataHandlers/indexer/main.py

import importlib
import json
from pathlib import Path

INDEXERS = [
    "dataHandlers.indexer.Beneficiaries_(PM_KISAN)_Indexer",
    "dataHandlers.indexer.agricultureSubSectorIndexer",
    "dataHandlers.indexer.TempAndRainfallIndexer",
]

SECTOR_MAP = {
    "Beneficiaries (PM-KISAN)": "dataHandlers/data/sectors/Beneficiaries_(PM_KISAN).json",
    "Agricultural Marketing": "dataHandlers/data/sectors/agricultural_marketing.json",
    "Crop Development & Seed Production": "dataHandlers/data/sectors/crop_development_and_seed_production.json",
    "Research, Education & Biotechnology": "dataHandlers/data/sectors/research,_education_and_biotechnology.json",
    "Temperature and Rainfall": "dataHandlers/data/sectors/temperature_and_rainfall.json",
}

def run_indexers():
    print("🌾 Starting all indexers...\n")
    for module_name in INDEXERS:
        print(f"🔹 Running {module_name}...")
        try:
            mod = importlib.import_module(module_name)
            if hasattr(mod, "main"):
                mod.main()
            elif hasattr(mod, "build"):
                mod.build()
            elif hasattr(mod, "build_index"):
                mod.build_index()
            else:
                print(f"⚠️  {module_name} has no callable entrypoint (expected main/build).")
        except Exception as e:
            print(f"❌ Error in {module_name}: {e}")
        print()

    # Write master index
    out_path = Path("dataHandlers/data/sectors/sector_index.json")
    out_path.write_text(json.dumps(SECTOR_MAP, indent=2))
    print(f"✅ Wrote master sector index → {out_path}")
    print("✅ All indexers completed.\n")

if __name__ == "__main__":
    run_indexers()
