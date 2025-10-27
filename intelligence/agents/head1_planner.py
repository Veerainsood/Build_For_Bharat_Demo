# intelligence/heads/head1_planner.py
import ollama

class Head1Planner:
    def __init__(self, model="mistral-nemo:12b"):
        self.model = model

    def make_prompt(self, query: str, datasets: list[dict]) -> str:
        """
        datasets: [
          {"name": "D1", "title": "...", "columns": ["SUBDIVISION","YEAR","ANNUAL"]},
          ...
        ]
        """
        desc = "\n".join(
            [f"{d['name']}: {d['title']} → {', '.join(d['columns'])}" for d in datasets]
        )
        prompt = f"""
            You are an analytical planner.
            Given a query and dataset metadata, write explicit numbered steps to answer the query.
            Use this compact line-based format with No Explanations or commentary:

            Example:
            1) D1[STATE,YEAR,RAINFALL_MM] filter STATE == 'Tamil Nadu'
            2) D1 group by YEAR compute mean(RAINFALL_MM)
            3) answer = trend(D1.mean_rainfall_mm)

            ---
            Query: {query}
            Datasets:
            {desc}
        """
        return prompt.strip()

    def plan(self, query: str, datasets: list[dict]) -> str:
        prompt = self.make_prompt(query, datasets)
        response = ollama.chat(model=self.model, messages=[{"role": "user", "content": prompt}])
        return response["message"]["content"].strip()
