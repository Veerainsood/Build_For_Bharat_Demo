# intelligence/heads/head1_planner.py
import ollama
from .selfCritique import DatasetRegistry
class Head1Planner:
    def __init__(self, model="mistral-nemo:12b"):
        self.model = model

    def make_prompt(self, query: str, registry: DatasetRegistry) -> str:
        """
        datasets: [
          {"name": "D1", "title": "...", "columns": ["SUBDIVISION","YEAR","ANNUAL"]},
          ...
        ]
        """
        prompt = f"""
            You are an analytical planner.
            Given a query and dataset metadata, write explicit numbered steps to answer the query.
            Use this compact line-based format with No Explanations or commentary.

            Example:
            1) D1[STATE,YEAR,RAINFALL_MM] filter STATE == 'Tamil Nadu'
            2) D1 group by YEAR compute mean(RAINFALL_MM)
            3) answer = trend(D1.mean_rainfall_mm)

            ---
            Query: {query}
            Here is a summary of available DataFrames:
            {registry.describe_all()}
        """
        return prompt.strip()

    def plan(self, query: str, registry: DatasetRegistry) -> str:
        prompt = self.make_prompt(query, registry=registry)
        breakpoint()
        response = ollama.chat(model=self.model, messages=[{"role": "user", "content": prompt}]) # 3min 33sec
        breakpoint()
        return response["message"]["content"].strip()
