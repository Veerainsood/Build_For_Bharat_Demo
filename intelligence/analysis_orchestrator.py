# intelligence/llm_tools/analysis_orchestrator.py
import json
from pathlib import Path
from .llm_tools.dataset_search_tool import DatasetSearchTool

class AnalysisOrchestrator:
    def __init__(self, sector_index_path: str = "dataHandlers/data/sector_index.json",
                 selector_model: str = "mistral-nemo:12b"):
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
    
    def select_files(self, query: str, selected_families: list[str]):
        """
        Given family/families (from stage 1) and query, select specific dataset files.
        Returns dict: {"selected_files": [...]}
        """
        from .llm_tools.file_selector import FileSearchTool
        from .llm_tools.pmKisanSelector import PMKisanSelector
        results = []
        for family in selected_families:
            path = Path(self.dataset_map[family])

            # special handling for PM-KISAN
            if "PM-KISAN" in family or "KISAN" in family:
                # breakpoint()
                data = json.load(open(path))
                tool = PMKisanSelector(data)
                res = tool.select(query)
                results.append({family: res})
                continue

            # normal selector (id, title based)
            data = json.load(open(path))
            tool = FileSearchTool(data)
            res = tool.select(query)
            results.append({family: res})

        return {"selected_files": results}
