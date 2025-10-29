from .analysis_orchestrator import AnalysisOrchestrator
from .agents.head1_planner import Head1Planner
from .llm_tools.dataframeFetcher import DataframeFetcher
from .agents.selfCritique import DatasetRegistry
from .analyzers.runAnaysis import Analyser
from .agents.head3_summarizer import Head3Answerer
from .llm_tools.ollama_utils import OllamaManager
import ollama

class Intellegence:

    def __init__(self):
        pass

    def get_response(self,query):
        
        orch = AnalysisOrchestrator(sector_index_path="dataHandlers/data/sectors/sector_index.json",selector_model="mistral-nemo:12b")
    
        planner = Head1Planner()
        
        res = orch.select_family(query)
        
        # to be returned
        return_selected_family = res
        return_selected_datasets = None
        return_registry = None
        return_head1_plan = None
        return_head2_plan = None
        return_head3_plan = None
        if res["selected_datasets"] != [-1]:
            
            files_res = orch.select_files(query, res["selected_datasets"])
            
            # to be retuned
            return_selected_datasets = files_res

            fetcher = DataframeFetcher()
            
            dfs = fetcher.fetch_selected_files(files_res)
            
            registry = DatasetRegistry()
            
            for i, (title, df) in enumerate(dfs.items(), start=1):
            
                name = f"D{i}"
            
                registry.register(name=name,df=df)

            plan = planner.plan(query=query, registry=registry)
            
            # registry
            return_registry = registry
            # return head 1's plan
            return_head1_plan = plan
            
            # saveFiles(registry=registry,plan=plan)
        # registry, plan, _ = loadFiles()

        with open("intelligence/analyzers/function_docs.txt") as f:
            tools_doc = f.read()
        # OllamaManager.stop_model("mistral-nemo:12b")
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
        breakpoint()
        response = ollama.chat(model="qwen2.5:14b", messages=[{"role": "user", "content": prompt}]) # 3min 30sec
        breakpoint()
        resp = response.get("message", {}).get("content", "").strip()
        
        return_head2_plan = resp
        # saveFiles(registry=registry, plan=plan, res=resp)

        analyst = Analyser()
        result = analyst.run_function_sequence(seq=resp, registry=registry)
        summarizer = Head3Answerer()
        breakpoint()
        results = summarizer.summarize_results(registry=registry,results=result,query=query)
        breakpoint()
        if results:
            pass
        else:
            results = "No results to summarize."

        return_head3_plan = results

        OllamaManager.stop_model("qwen2.5:14b")

        return return_head1_plan , return_head2_plan ,return_head3_plan , return_registry , return_selected_datasets , return_selected_family


if __name__ == '__main__':
    intellegent = Intellegence()
    queries = [
        "Compare the average annual rainfall in Tamil Nadu and Kerala for the last 5 available years. In parallel, list the top 5 most produced food crops (by volume) in each of those states during the same period, citing all data sources.",
        "crop varieties released for wheat",
        "Identify the district in Maharashtra with the highest production of Sugarcane in the most recent year available and compare that with the district with the lowest production of Sugarcane in Karnataka.",
        "beneficiaries under PM-KISAN in Nicobar district for 2022-23",
        "Analyze the production trend of Rice in Eastern Uttar Pradesh over the last decade. Correlate this trend with the corresponding rainfall data for the same period and summarize the apparent impact.",
        "A policy advisor is proposing a scheme to promote Millet (drought-resistant) over Paddy (water-intensive) in Telangana. Based on historical data from the last 10 years, list three strong data-backed arguments to support this policy, using both climate and agricultural datasets."
    ]

    for query in queries:
        head1_plan , head2_plan ,head3_plan , registry , selected_datasets , selected_family = intellegent.get_response(query=query)
        print(head3_plan)
