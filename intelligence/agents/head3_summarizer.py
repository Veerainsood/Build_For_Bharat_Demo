import ollama
import pandas as pd
from ..agents.selfCritique import DatasetRegistry
class Head3Answerer:
    def __init__(self, model="mistral-nemo:12b"):
        self.model = model

    def summarize_results(self, registry : DatasetRegistry ,results: dict, query: str):
        # pick the last non-empty DataFrame
        try:
            dfs = [v for v in results.values() if isinstance(v, pd.DataFrame) and not v.empty]
        except Exception:
            pass
        # breakpoint()
        if not dfs:
            if isinstance(registry.datasets, dict):
                # take all dataframes
                dfs = [df for df in registry.datasets.values() if not df.empty]
                # or concatenate them if you want one unified df
                dfs = pd.concat(dfs, axis=0, ignore_index=True)
            else:
                dfs = registry.datasets
                # return "No valid results were produced."

        try:
            last_df = dfs[-1]
        except Exception:
            last_df = dfs
        # make small markdown snapshot
        sample = last_df.head(8).to_markdown(index=False)

        prompt = f"""
        You are an analytical assistant.
        The following table is the final analytical output of a local data analysis pipeline.
        The user originally asked: "{query}"

        Here are the final results:

        {sample}

        Write 3–4 short, factual insights that can be derived from this data.
        If numerical trends are visible, mention them.
        Do NOT say that data is insufficient or uncertain.
        End with one clear concluding statement.
        """

        response = ollama.chat(model=self.model, messages=[{"role": "user", "content": prompt}])
        return response["message"]["content"]