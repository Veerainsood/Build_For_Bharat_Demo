# bharatgpt/demo/demo_scraper.py
from ..connectors.ogdp_scraper import OGDPScraper
from ..indexer.metadata_index import MetadataIndex

def run_scraper():
    index = MetadataIndex()
    targets = [
        ("Agriculture", ["Ministry of Agriculture and Farmers Welfare"]),
        (None, [  # no sector for IMD datasets
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

if __name__ == "__main__":
    run_scraper()
