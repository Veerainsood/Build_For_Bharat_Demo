# bharatgpt/demo/demo_scraper.py
from .ogdp_scraper import OGDPScraper
from ..indexer.metadata_index import MetadataIndex

def run_scraper():
    index = MetadataIndex()
    
    targets = [
        # --- Agriculture umbrella ---
        ("Agriculture", ["Ministry of Agriculture and Farmers Welfare"]),
        ("Agricultural Marketing", ["Ministry of Agriculture and Farmers Welfare"]),
        ("Agricultural Produces", ["Ministry of Agriculture and Farmers Welfare"]),
        ("Agricultural Research & Extension", ["Ministry of Agriculture and Farmers Welfare"]),
        ("Crops", ["Ministry of Agriculture and Farmers Welfare"]),
        ("Horticulture", ["Ministry of Agriculture and Farmers Welfare"]),
        ("Seeds", ["Ministry of Agriculture and Farmers Welfare"]),
        ("Biotechnology", ["Ministry of Agriculture and Farmers Welfare"]),
        ("Education", ["Ministry of Agriculture and Farmers Welfare"]),

        # --- IMD / MoES umbrella ---
        (None , [
            "India Meteorological Department (IMD)",
            "Ministry of Earth Sciences",
            "India Meteorological Department (IMD), Pune"
        ]),
    ]

    for sector, ministries in targets:
        print(f"Fetching {sector} / {ministries}...")
        all_rows = []
        for m in ministries:
            scraper = OGDPScraper(sector=sector, ministries=[m], limit=100, exact_match=True)
            rows = scraper.crawl_all(max_pages=110)
            print(f"   → {len(rows)} datasets found for {m}")
            all_rows.extend(rows)
        # deduplicate by id
        seen = {}
        for r in all_rows:
            seen[r["id"]] = r
        print(f"   ✅ Total unique: {len(seen)}")
        for r in seen.values():
            if r["id"]:
                index.upsert_dataset(r)
        print(" ✓ Done.\n")
    # Special PM-KISAN index
    

if __name__ == "__main__":
    run_scraper()
    # do indexing via calling main() in indexer modules separately
    # or via dataHandlers/indexer/main.py
    print("✅ Scraper run completed.")


    print("🌾 Running indexers...")
    from  ..indexer.main import run_indexers
    run_indexers()
    