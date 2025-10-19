# intelligence/llm_tools/analysis_orchestrator.py
import json
from pathlib import Path
from .llm_tools.dataset_search_tool import DatasetSearchTool

class AnalysisOrchestrator:
    def __init__(self, sector_index_path: str = "dataHandlers/data/sector_index.json",
                 selector_model: str = "phi3:mini"):
        self.sector_index_path = sector_index_path
        self.dataset_map = self._load_sector_index(Path(sector_index_path))
        self.dataset_selector = DatasetSearchTool(self.dataset_map, model=selector_model)

    def _load_sector_index(self, p: Path) -> dict:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Expect a flat mapping: { family_name: path }
        if not isinstance(data, dict):
            raise ValueError("sector_index.json must be a dict of {family: path}")
        return data

    def select_family(self, user_query: str) -> dict:
        """
        Returns:
          {"selected_datasets": ["<family>"]} or {"selected_datasets": [-1]}
        """
        return self.dataset_selector.select(user_query)
