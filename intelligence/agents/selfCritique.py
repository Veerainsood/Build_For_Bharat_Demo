"""
Self-Critique Agent
-------------------
Lightweight feedback-driven executor for Build for Bharat.
Integrates:
 - DatasetRegistry (shared DataFrame memory)
 - SandboxExecutor (isolated Python runner)
 - LLM feedback loop (DeepSeek-Coder or similar)
"""

import io, json, traceback, contextlib, re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ollama


class DatasetRegistry:
    """Keeps named DataFrames accessible to both models and sandbox."""

    def __init__(self):
        self.datasets = {}

    def register(self, name: str, df: pd.DataFrame):
        df = df.copy()
        df.replace(["NA", "NaN", "", " "], np.nan, inplace=True)

        # numeric imputation
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isna().any():
                df[col].fillna(df[col].mean(), inplace=True)

        # categorical imputation
        cat_cols = df.select_dtypes(exclude=[np.number]).columns
        for col in cat_cols:
            if df[col].isna().any():
                df[col].fillna(df[col].mode(), inplace=True)

        self.datasets[name] = df

    def get(self, name: str) -> pd.DataFrame:
        return self.datasets.get(name)

    def describe_all(self, max_cols=8, max_rows=2, max_uniques=10):
        summary = {}
        for name, df in self.datasets.items():
            cols = list(df.columns)[:max_cols]
            desc = {
                "shape": df.shape,
                "columns": cols,
                "sample": df.head(max_rows).to_dict(orient="records")
            }
            uniques = {}
            for c in cols:
                if df[c].dtype == "object":
                    vals = df[c].dropna().unique()[:max_uniques]
                    uniques[c] = vals.tolist() if hasattr(vals, "tolist") else list(vals)
            if uniques:
                desc["unique_values"] = uniques
            summary[name] = desc
        return json.dumps(summary, indent=2)


class SandboxExecutor:
    """Runs arbitrary Python code safely in an isolated namespace with DataFrames."""

    def __init__(self, registry: DatasetRegistry):
        self.registry = registry
        self.env = {"pd": pd, "np": np, "plt": plt}
        self.env.update(registry.datasets)

    def run(self, code: str):
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                exec(code, self.env)
            return {"success": True, "stdout": buf.getvalue(), "error": None}
        except Exception:
            return {"success": False, "stdout": buf.getvalue(), "error": traceback.format_exc()}

    def sync_registry(self):
        """Sync back modified DataFrames."""
        for name, val in self.env.items():
            if isinstance(val, pd.DataFrame):
                self.registry.register(name, val)


class SelfCritiqueAgent:
    """
    Orchestrates: code generation → execution → critique → repair.
    """

    def __init__(self, coder_model="qwen2.5:14b", max_loops=3):
        self.coder_model = coder_model
        self.max_loops = max_loops

    def _chat(self, prompt: str) -> str:
        """Wrapper for Ollama chat call."""
        resp = ollama.chat(model=self.coder_model, messages=[{"role": "user", "content": prompt}])
        return resp.get("message", {}).get("content", "").strip()

    def _extract_code(self, text: str) -> str:
        """Grab code fences or fallback to raw text."""
        if "```python" in text:
            return text.split("```python")[-1].split("```")[0].strip()
        if "```" in text:
            return text.split("```")[-1].strip()
        return text.strip()

    def run_loop(self, initial_prompt: str, registry: DatasetRegistry):
        """
        Main feedback loop: generate → run → critique → repair.
        """
        sandbox = SandboxExecutor(registry)

        # ---- first generation ----
        code = self._chat(initial_prompt)
        code = self._extract_code(code)

        for attempt in range(1, self.max_loops + 1):
            print(f"\n===================== Attempt {attempt} =====================")
            result = sandbox.run(code)
            sandbox.sync_registry()
            # breakpoint()
            if result["success"]:
                print("✅ Execution successful.")
                print(result["stdout"])
                return code, result

            #            # --- failed: prepare feedback ---
            feedback = f"""
            The previous Python code failed to execute.

            Here is the full code that failed:
            {code}

            Here is the traceback (truncated):
            {result['error'][:1000] if isinstance(result, dict) and 'error' in result else result}

            Here is a summary of available DataFrames:
            {registry.describe_all()}

            Instructions:
            - DO NOT redefine or reload any DataFrame (D1–D5); they are already in memory.
            - You must produce a fresh, fully runnable Python script that fixes the above error.
            - The script must execute end-to-end without manual data loading.
            - Always print or return the final result variable.
            - Output ONLY the corrected Python code (no explanations or comments).
            """


            print("⚠️ Execution failed, feeding back to coder model...")
            code = self._chat(feedback)
            code = self._extract_code(code)

        print("❌ Max retries reached. Returning last attempt.")
        return code, result


# ---------------------------------------------------------------
if __name__ == "__main__":
    # === Demo setup ===
    registry = DatasetRegistry()
    registry.register("D1", pd.DataFrame({
        "YEAR": [2020, 2021, 2022, 2023],
        "JUN_SEP": [100, 120, 140, 160]
    }))

    prompt = """
    You are given a pandas DataFrame D1 with columns YEAR and JUN_SEP.
    Write Python code to:
    1. Filter rows where YEAR > 2020
    2. Compute the mean of JUN_SEP grouped by YEAR
    3. Print the resulting DataFrame
    """

    agent = SelfCritiqueAgent(coder_model="deepseek-coder:6.7b", max_loops=5)
    final_code, result = agent.run_loop(prompt, registry)

    print("\n=== FINAL CODE ===")
    print(final_code)