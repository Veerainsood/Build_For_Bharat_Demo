# scripts/test_dataset_selection.py
from .analysis_orchestrator import AnalysisOrchestrator
from .llm_tools.ollama_utils import OllamaManager

from dataHandlers.fetchers.data_fetcher import DataFetcher

def fetch_selected_files(selected_files_dict):
    """
    Takes the exact structure printed by your Stage-2 output.
    Returns: dict[file_title -> DataFrame]
    """
    fetcher = DataFetcher()
    results = {}

    for family_entry in selected_files_dict.get("selected_files", []):
        for family_name, family_data in family_entry.items():
            for file_entry in family_data.get("selected_files", []):
                try:
                    df = fetcher.load_any(file_entry)
                    title = file_entry.get("title") or file_entry.get("id") or str(file_entry)
                    results[title] = df
                except Exception as e:
                    print(f"⚠️ Skipped {file_entry}: {e}")

    return results


if __name__ == "__main__":
    orch = AnalysisOrchestrator(sector_index_path="dataHandlers/data/sectors/sector_index.json",
                                selector_model="mistral-nemo:12b")
    queries = [
        # "rainfall trend in tamil nadu over the last 3 years",
        # "impact of rainfall on crop yields in tamil nadu",
        # "Compare the average annual rainfall in State_X and State_Y for the last N available years. In parallel, list the top M most produced crops of Crop_Type_C (by volume) in each of those states during the same period, citing all data sources."
        # "crop varieties released for wheat",
        # "Identify the district in State_X with the highest production of Crop_Z in the most recent year available and compare that with the district with the lowest production of Crop_Z  in State_Y?",
        "beneficiaries under pm-kisan in Nicobars 2022-23",
        # "Analyze the production trend of Crop_Type_C in the Geographic_Region_Y over the last decade. Correlate this trend with the corresponding climate data for the same period and provide a summary of the apparent impact.",
        # "A policy advisor is proposing a scheme to promote Crop_Type_A (e.g., drought-resistant) over Crop_Type_B (e.g., water-intensive) in Geographic_Region_Y. Based on historical data from the last N years, what are the three most compelling data-backed arguments to support this policy? Your answer must synthesize data from both climate and agricultural sources."
    ]
    for q in queries:
        # breakpoint()
        res = orch.select_family(q)
        print(q, "->", res)
        if res["selected_datasets"] != [-1]:
            files_res = orch.select_files(q, res["selected_datasets"])
            print()
            print("############################################################")
            print("  Selected files:", files_res)
            print("############################################################")
            print()
            # breakpoint()
            dfs = fetch_selected_files(files_res)
            for title, df in dfs.items():
                print(f"--- DataFrame: {title} ---")
                print(df.head())
                print()
            
    OllamaManager.stop_model("mistral-nemo:12b")