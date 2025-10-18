# dataHandlers/indexer/dataset_selector.py
import duckdb
from typing import List, Dict

class DatasetSelector:
    """Searches the local DuckDB metadata index for relevant datasets."""
    def __init__(self, db_path="dataHandlers/data/ogdp_index.db"):
        self.db_path = db_path
        # persistent connection so we don’t reopen every query
        self.con = duckdb.connect(self.db_path, read_only=True)

    def search(self, query: str, limit: int = 10) -> list[dict]:
        """Loose keyword search in title and note fields (plural-safe)."""
        terms = [query.lower()]
        # add plural/synonyms
        if query.lower() == "crop":
            terms += ["crops", "agriculture", "icar", "variety", "varieties"]
        elif query.lower() == "rainfall":
            terms += ["rain", "precipitation"]
        elif query.lower() == "fertilizer":
            terms += ["fertiliser", "nutrient", "urea"]
        elif query.lower() == "portability":
            terms += ["onorc", "ration", "nfsa"]

        like_clauses = " OR ".join(
            [f"lower(title) LIKE '%{t}%'" for t in terms] +
            [f"lower(note) LIKE '%{t}%'" for t in terms]
        )
        q = f"SELECT * FROM datasets WHERE {like_clauses} LIMIT {limit};"

        con = duckdb.connect(self.db_path, read_only=True)
        rows = con.execute(q).fetchall()
        cols = [desc[0] for desc in con.description]
        return [dict(zip(cols, row)) for row in rows]

