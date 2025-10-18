from dataHandlers.indexer.dataset_selector import DatasetSelector

selector = DatasetSelector()

for query in ["rainfall", "crop", "portability", "fertilizer"]:
    print(f"\n=== {query.upper()} ===")
    results = selector.search(query, limit=5)
    for r in results:
        print("-", r["title"])
        print(" ", r["id"])
        print()