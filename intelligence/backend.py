from .analysis_orchestrator import AnalysisOrchestrator
from .agents.head1_planner import Head1Planner
from .llm_tools.dataframeFetcher import DataframeFetcher
from .agents.selfCritique import DatasetRegistry
from .analyzers.runAnaysis import Analyser
from .agents.head3_summarizer import Head3Answerer
from .llm_tools.ollama_utils import OllamaManager
import ollama
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import json
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # or ["http://localhost:5173"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class Intellegence:

    def __init__(self):
        pass

    def get_response(self,query):
        
        def send(event_type, data):
            return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
        
        yield send("status", {"stage": "init", "message": "Starting analysis..."})
        yield send("next", {"stage": "family", "message": "Selecting dataset family..."})
        orch = AnalysisOrchestrator(sector_index_path="dataHandlers/data/sectors/sector_index.json",selector_model="mistral-nemo:12b")
    
        planner = Head1Planner()
        
        res = orch.select_family(query)
        yield send("family", res)
        # to be returned
        return_selected_family = res
        return_selected_datasets = None
        return_registry = None
        return_head1_plan = None
        return_head2_plan = None
        return_head3_plan = None
        if res["selected_datasets"] == [-1]:
            yield send("done", {"error": "No datasets found"})
            return
        
        yield send("next", {"stage": "datasets", "message": "Selecting datasets..."})    
        files_res = orch.select_files(query, res["selected_datasets"])
        print(files_res)
        yield send("datasets", files_res)
        yield send("next", {"stage": "registry", "message": "Fetching and registering DataFrames..."})
        # to be retuned
        return_selected_datasets = files_res

        fetcher = DataframeFetcher()
        
        dfs = fetcher.fetch_selected_files(files_res)
        
        registry = DatasetRegistry()
        
        for i, (title, df) in enumerate(dfs.items(), start=1):
        
            name = f"D{i}"
        
            registry.register(name=name,df=df)
        
        preview_data = {}
        for name, df in registry.datasets.items():
            try:
                preview_data[name] = df.head(5).to_dict(orient="records")
            except Exception:
                preview_data[name] = "Preview unavailable"

        yield send("registry", {"previews": preview_data})
        yield send("next", {"stage": "head1", "message": "Generating high-level analytical plan..."})
        plan = planner.plan(query=query, registry=registry)
        
        # registry
        return_registry = registry
        # return head 1's plan
        return_head1_plan = plan
        yield send("head1", {"plan": plan})
        yield send("next", {"stage": "head2", "message": "Converting plan to executable steps..."})
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
        response = ollama.chat(model="qwen2.5:14b", messages=[{"role": "user", "content": prompt}]) # 3min 30sec
        resp = response.get("message", {}).get("content", "").strip()
        
        return_head2_plan = resp
        yield send("head2", {"operations": resp})
        yield send("next", {"stage": "head3", "message": "Summarizing and synthesizing final answer..."})
        # saveFiles(registry=registry, plan=plan, res=resp)

        analyst = Analyser()
        result = analyst.run_function_sequence(seq=resp, registry=registry)
        summarizer = Head3Answerer()
        results = summarizer.summarize_results(registry=registry,results=result,query=query)
        
        if results:
            pass
        else:
            results = "No results to summarize."

        return_head3_plan = results
        yield send("head3", {"summary": results})
        yield send("done", {"message": "Analysis complete"})
        yield send("next", {"stage": "done", "message": "Finalizing and cleaning up models..."})

        OllamaManager.stop_model("qwen2.5:14b")

intel = Intellegence()
@app.get("/query")
def query_endpoint(query: str):
    return StreamingResponse(intel.get_response(query), media_type="text/event-stream")


# if __name__ == '__main__':
#     intellegent = Intellegence()
    # queries = [
    #     # 1️⃣ Climate–Agriculture comparison
    #     "Compare the average annual rainfall in Tamil Nadu and Kerala for the last 5 available years. In parallel, list the top 5 most produced food crops by volume in each of those states during the same period, with source citations from IMD and the Department of Agriculture.",

    #     # 2️⃣ Crop variety lookup
    #     "List all crop varieties released for Wheat under the 'Crop Development & Seed Production' sector, including the year of release and certifying institute.",

    #     # 3️⃣ District-level production comparison (if available)
    #     "Identify the district in Maharashtra with the highest sugarcane production in the most recent year available and compare it with the district in Karnataka that recorded the lowest sugarcane production for the same period.",

    #     # 4️⃣ Scheme beneficiary stats
    #     "Report the number of beneficiaries registered under the PM-KISAN scheme in Nicobar district for the financial year 2022–23.",

    #     # 5️⃣ Trend + correlation (multi-dataset)
    #     "Analyze the production trend of Rice in Eastern Uttar Pradesh over the last decade. Correlate this trend with annual rainfall data from IMD for the same region and summarize the apparent impact.",

    #     # 6️⃣ Policy analysis query
    #     "Evaluate a policy proposal to promote Millets (drought-resistant) over Paddy (water-intensive) in Telangana. Using the last 10 years of agricultural and climate data, provide three strong data-backed arguments supporting this policy, citing all sources."
    # ]


#     for query in queries:
#         print()
#         print("Query -> ",query)
#         head1_plan , head2_plan ,head3_plan , registry , selected_datasets , selected_family = intellegent.get_response(query=query)
#         print(head3_plan)
