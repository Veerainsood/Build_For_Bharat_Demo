# scripts/test_dataset_selection.py
from .analysis_orchestrator import AnalysisOrchestrator

if __name__ == "__main__":
    orch = AnalysisOrchestrator(sector_index_path="dataHandlers/data/sectors/sector_index.json",
                                selector_model="mistral-nemo:12b")
    queries = [
        "rainfall trend in tamil nadu over the last 3 years",
        "impact of rainfall on crop yields in tamil nadu",
        "Compare the average annual rainfall in State_X and State_Y for the last N available years. In parallel, list the top M most produced crops of Crop_Type_C (by volume) in each of those states during the same period, citing all data sources."
        "crop varieties released for wheat",
        "Identify the district in State_X with the highest production of Crop_Z in the most recent year available and compare that with the district with the lowest production of Crop_Z  in State_Y?",
        "beneficiaries under pm-kisan in Nicobars 2022-23",
        "Analyze the production trend of Crop_Type_C in the Geographic_Region_Y over the last decade. Correlate this trend with the corresponding climate data for the same period and provide a summary of the apparent impact.",
        "A policy advisor is proposing a scheme to promote Crop_Type_A (e.g., drought-resistant) over Crop_Type_B (e.g., water-intensive) in Geographic_Region_Y. Based on historical data from the last N years, what are the three most compelling data-backed arguments to support this policy? Your answer must synthesize data from both climate and agricultural sources."
    ]
    for q in queries:
        # breakpoint()
        res = orch.select_family(q)
        print(q, "->", res)