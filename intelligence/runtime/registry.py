# intelligence/runtime/registry.py
from dataclasses import dataclass
from datetime import datetime
import hashlib
import pandas as pd

@dataclass
class DFMeta:
    source_id: str
    title: str
    url: str
    file_format: str
    retrieved_at: str

def make_meta(url: str, title: str | None = None) -> DFMeta:
    sid = hashlib.md5(url.encode()).hexdigest()[:10]
    ext = url.split("?")[0].split(".")[-1].lower()
    return DFMeta(
        source_id=sid,
        title=title or url.split("/")[-1],
        url=url,
        file_format=ext,
        retrieved_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
    )

class Registry:
    """name -> (DataFrame, DFMeta)"""
    def __init__(self): self.store = {}

    def add(self, name: str, df: pd.DataFrame, meta: DFMeta):
        self.store[name] = (df, meta)

    def get(self, name: str) -> tuple[pd.DataFrame, DFMeta]:
        return self.store[name]

    def used_citations(self, used_names: list[str]) -> list[DFMeta]:
        return [self.store[n][1] for n in used_names if n in self.store]
