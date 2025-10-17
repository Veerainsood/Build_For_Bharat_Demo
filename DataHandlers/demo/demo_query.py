# bharatgpt/demo/demo_query.py
from ..indexer.dataset_selector import DatasetSelector

def run_query():
    selector = DatasetSelector()
    query = input("Enter your query: ")
    results = selector.search(query)
    print(f"\nTop matches for '{query}':\n")
    for r in results:
        print(f"- {r['title']}")
        print(f"  {r['format']} → {r['id']}")
        print(f"  Ministry: {r['ministry']}")
        print("")

if __name__ == "__main__":
    run_query()
