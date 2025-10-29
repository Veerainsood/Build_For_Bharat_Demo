# scripts/test_dataset_selection.py
from ..analysis_orchestrator import AnalysisOrchestrator
from .ollama_utils import OllamaManager
from ..agents.head1_planner import Head1Planner
from dataHandlers.fetchers.data_fetcher import DataFetcher
import json
from ..agents.selfCritique import SelfCritiqueAgent
from ..agents.selfCritique import DatasetRegistry
from ..analyzers.runAnaysis import Analyser
from ..agents.head3_summarizer import Head3Answerer

class DataframeFetcher:

    def __init__(self):
        pass

    def fetch_selected_files(self,selected_files_dict):
        """
        Takes the exact structure printed by your Stage-2 output.
        Returns: dict[file_title -> DataFrame]
        """
        fetcher = DataFetcher()
        results = {}
        # breakpoint()
        for family_entry in selected_files_dict.get("selected_files", []):
            for family_name, family_data in family_entry.items():
                for entry in family_data.get("selected_files", []):
                    
                    if isinstance(entry,int) and entry == -1:
                        continue
                    
                    try:
                        df = fetcher.load_any(family_name,entry)
                        title = entry.get("title") or entry.get("id") or str(entry)
                        results[title] = df
                    except Exception as e:
                        print(f"⚠️ Skipped {entry}: {e}")

        return results

    def saveFiles(self,registry,plan,res=""):
        import pickle

        with open("cache/registry.pkl", "wb") as f:
            pickle.dump(registry, f)

        # --- Save dataset metadata as JSON
        # with open("cache/dataset_meta.json", "w") as f:
        #     json.dump(dataset_meta, f, indent=2, ensure_ascii=False)

        # --- Optionally also save the plan from Head-1
        with open("cache/head1_plan.txt", "w") as f:
            f.write(plan if isinstance(plan, str) else "\n".join(map(str, plan)))

        with open("cache/head2_plan.txt" , "w") as f:
            f.write(res if isinstance(res, str) else "\n".join(map(str, plan)))

    def loadFiles(self):
        import pickle, json

        with open("cache/registry.pkl", "rb") as f:
            registry = pickle.load(f)

        # with open("cache/dataset_meta.json") as f:
        #     dataset_meta = json.load(f)

        with open("cache/head1_plan.txt") as f:
            plan = f.read().splitlines()
        
        with open("cache/head2_plan.txt") as f:
            resp = f.read()

        return registry , plan , resp
