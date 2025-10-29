# scripts/test_dataset_selection.py
from .analysis_orchestrator import AnalysisOrchestrator
from .llm_tools.ollama_utils import OllamaManager
from .agents.head1_planner import Head1Planner
from dataHandlers.fetchers.data_fetcher import DataFetcher
import json
from .agents.selfCritique import SelfCritiqueAgent
from .agents.selfCritique import DatasetRegistry
from .analyzers.runAnaysis import Analyser
from .agents.head3_summarizer import Head3Answerer
def fetch_selected_files(selected_files_dict):
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

def saveFiles(registry,plan,res=""):
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

def loadFiles():
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


if __name__ == "__main__":
    orch = AnalysisOrchestrator(sector_index_path="dataHandlers/data/sectors/sector_index.json",
                                selector_model="mistral-nemo:12b")
    
    planner = Head1Planner()
    queries = [
        "rainfall trend in Tamil Nadu over the last 3 years",
        "impact of rainfall on crop yields in Tamil Nadu",
        "Compare the average annual rainfall in Tamil Nadu and Kerala for the last 5 available years. In parallel, list the top 5 most produced food crops (by volume) in each of those states during the same period, citing all data sources.",
        "crop varieties released for wheat",
        "Identify the district in Maharashtra with the highest production of Sugarcane in the most recent year available and compare that with the district with the lowest production of Sugarcane in Karnataka.",
        "beneficiaries under PM-KISAN in Nicobar district for 2022-23",
        "Analyze the production trend of Rice in Eastern Uttar Pradesh over the last decade. Correlate this trend with the corresponding rainfall data for the same period and summarize the apparent impact.",
        "A policy advisor is proposing a scheme to promote Millet (drought-resistant) over Paddy (water-intensive) in Telangana. Based on historical data from the last 10 years, list three strong data-backed arguments to support this policy, using both climate and agricultural datasets."
    ]

    for q in queries:
        # breakpoint()
        res = orch.select_family(q)
        print(q, "->", res)
        # breakpoint()
        if res["selected_datasets"] != [-1]:
            files_res = orch.select_files(q, res["selected_datasets"])
            print()
            print("############################################################")
            print("Selected files:", files_res)
            print("############################################################")
            print()
            # breakpoint()
            dfs = fetch_selected_files(files_res)
            registry = DatasetRegistry()
            for i, (title, df) in enumerate(dfs.items(), start=1):
                name = f"D{i}"
                registry.register(name=name,df=df)
                cols = list(df.columns)[:8]  # keep first few for brevity
                print(f"--- DataFrame: {title} ---")
                print(df.head())
                print()
            # breakpoint()
            plan = planner.plan(query=q, registry=registry)
            print(registry.describe_all())
            print("=== HEAD-1 PLAN ===")
            print(plan)
            print()
            # saveFiles(registry=registry,plan=plan)
        # registry, plan, _ = loadFiles()
        print("=== HEAD-2 OUTPUT ===")

        with open("intelligence/analyzers/function_docs.txt") as f:
            tools_doc = f.read()

        prompt = f"""
        You are an analysis planner.
        Use only the following functions:

        {tools_doc}

        Input datasets:
        {registry.describe_all()}

        Assume all datasets end in the year 2022.
        If you need to refer to 'current year', use 2018 instead of datetime.now() or max(YEAR).

        Plan:
        {plan}

        Output a JSON array of operations.
        Each operation must strictly follow this format:
        ["output_name", "function_name", "input_name", {{"arg1": value1, "arg2": value2}}]

        Rules:
        - "output_name" is a short label for storing this step’s result (e.g., "filtered", "year_avg", "joined").
        - "input_name" can refer to a dataset name or a previously defined output_name.
        - Use only dataset or output names that exist earlier in the sequence.
        - Do not add explanations or comments — only pure JSON array output.
        """

        # === Step 3: Convert plan → executable sequence ===
        agent = SelfCritiqueAgent(coder_model="qwen2.5:14b", max_loops=5)
        resp = agent._chat(prompt=prompt)
        saveFiles(registry=registry, plan=plan, res=resp)
        # registry , plan , resp = loadFiles()

        analyst = Analyser()
        # breakpoint()
        result = analyst.run_function_sequence(seq=resp, registry=registry)
        # print(result)

        summarizer = Head3Answerer()
        results = summarizer.summarize_results(registry=registry,results=result,query=q)
        if results:
            print("\n=== HEAD-3 INSIGHTS ===\n")
            print(results)
        else:
            print("No results to summarize.")
        # prompt = f"""
        # You are a Python data-analysis agent.

        # You are given the following in-memory pandas DataFrames:
        # {json.dumps(dataset_meta, indent=2)}

        # Each entry lists a dataset name (D1, D2, …) with its title and columns.

        # Follow this plan step-by-step:
        # {plan}

        # Write a complete Python script that:
        # - uses the existing DataFrames (D1, D2, …) directly — do NOT read CSVs or redefine them
        # - performs the specified operations exactly as described
        # - prints key intermediate results and the final output clearly
        # - uses standard pandas and numpy operations only

        # Output only the full runnable Python code — no markdown fences, no explanations.
        # """

        # # === Step 4: Run the self-critique agent ===
        # final_code, result = agent.run_loop(prompt, registry)

        # print("\n=== FINAL CODE ===")
        # print(final_code)
        # print(json.dumps(ops, indent=2))
        
    OllamaManager.stop_model("qwen2.5:14b")