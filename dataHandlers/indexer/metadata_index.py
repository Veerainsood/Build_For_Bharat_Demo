import duckdb
from pathlib import Path
from datetime import datetime
from typing import Dict

class MetadataIndex:
    """Manages local DuckDB index of dataset metadata."""
    def __init__(self, db_path="dataHandlers/data/ogdp_index.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(db_path)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            id TEXT PRIMARY KEY,
            title TEXT,
            note TEXT,
            sector TEXT,
            ministry TEXT,
            granularity TEXT,
            format TEXT,
            ref_url TEXT,
            scraped_at TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

    def upsert_dataset(self, data: Dict):
        """Insert or update a single dataset record (DuckDB compatible)."""
        self.con.execute("""
            INSERT INTO datasets AS d (id, title, note, sector, ministry, granularity, format, ref_url, scraped_at, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE
                SET title = excluded.title,
                    note = excluded.note,
                    sector = excluded.sector,
                    ministry = excluded.ministry,
                    granularity = excluded.granularity,
                    format = excluded.format,
                    ref_url = excluded.ref_url,
                    scraped_at = excluded.scraped_at,
                    last_seen = now();
        """, [
            data.get("id"),
            data.get("title"),
            data.get("note"),
            data.get("sector"),
            data.get("ministry"),
            data.get("granularity"),
            data.get("format"),
            data.get("ref_url"),
            data.get("scraped_at"),
            datetime.utcnow()
        ])

    def all_datasets(self):
        return self.con.execute("SELECT * FROM datasets").fetchdf()