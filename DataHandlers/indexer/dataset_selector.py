# DataHandlers/indexer/dataset_selector.py
import duckdb
from typing import List, Dict

class DatasetSelector:
    """Searches the local DuckDB metadata index for relevant datasets."""
    def __init__(self, db_path="DataHandlers/data/ogdp_index.db"):
        self.db_path = db_path
        # persistent connection so we don’t reopen every query
        self.con = duckdb.connect(self.db_path, read_only=True)

    def search(self, query: str, limit: int = 10) -> List[Dict]:
        """Keyword-based search in title and note fields."""
        like_pattern = f"%{query.lower()}%"
        q = """
        SELECT id, title, note, ministry, sector, granularity, format, ref_url
        FROM datasets
        WHERE lower(title) LIKE ?
           OR lower(note) LIKE ?
        LIMIT ?;
        """
        result = self.con.execute(q, [like_pattern, like_pattern, limit]).fetchall()
        cols = [desc[0] for desc in self.con.description]
        return [dict(zip(cols, row)) for row in result]
