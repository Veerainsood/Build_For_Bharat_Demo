# intelligence/heads/head2_grounder.py
import ollama

class Head2Grounder:
    def __init__(self, model="phi3-mini"):
        self.model = model

    def make_prompt(self, plan: str, query: str, dataset_values: dict) -> str:
        """
        dataset_values: {
          "D1": {
            "column": "SUBDIVISION",
            "values": {
              "#001": "Andaman & Nicobar Islands",
              "#002": "Chandigarh, Punjab & Delhi",
              "#003": "Tamil Nadu"
            }
          }
        }
        """
        val_text = "\n".join(
            [
                f"{d} {meta['column']}:\n" +
                "\n".join([f"  {k}: {v}" for k, v in meta["values"].items()])
                for d, meta in dataset_values.items()
            ]
        )
        prompt = f"""
            You are the grounding module.
            You receive a query, a plan of analytical steps, and indexed unique row names for datasets.
            Your task: rewrite the plan replacing text filters with the appropriate IDs.
            If a query term appears as part of a combined row name, map it to that ID.

            Query: {query}

            Available indexed values:
            {val_text}

            Plan to ground:
            {plan}

            Respond only with the revised numbered plan using IDs (e.g., #002) and variable bindings.No Explanations or commentary whatsoever.
        """
        return prompt.strip()

    def ground(self, query: str, plan: str, dataset_values: dict) -> str:
        prompt = self.make_prompt(plan, query, dataset_values)
        response = ollama.chat(model=self.model, messages=[{"role": "user", "content": prompt}])
        return response["message"]["content"].strip()
