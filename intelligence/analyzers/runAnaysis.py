import json
import pandas as pd
import re
from . import function_lib
from ..agents.selfCritique import DatasetRegistry
import traceback, os

# --- JSON helpers ---

def normalize_ops(ops):
    """Flatten single-element lists for 'input_name'."""
    corrected = []
    for op in ops:
        if len(op) == 4 and isinstance(op[2], list):
            if len(op[2]) == 1:
                op[2] = op[2][0]
        corrected.append(op)
    return corrected


def safe_json_loads(text: str):
    """Try to robustly parse LLM output into valid JSON."""
    text = text.strip()
    text = re.sub(r"^```(json|python)?", "", text)
    text = re.sub(r"```$", "", text)
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Fallback: coerce to valid JSON
    fixed = (
        text.replace("'", '"')
        .replace("True", "true")
        .replace("False", "false")
        .replace("None", "null")
    )
    fixed = re.sub(r",(\s*[\]\}])", r"\1", fixed)

    try:
        return json.loads(fixed)
    except Exception as e:
        raise ValueError(f"JSON correction failed: {e}\nRaw text:\n{text}")


# --- Executor class ---

class Analyser:
    """Executes JSON-based operation sequences (Head-2 output) with self-correction."""

    def __init__(self):
        self.lib = {
            name: getattr(function_lib, name)
            for name in dir(function_lib)
            if callable(getattr(function_lib, name)) and not name.startswith("_")
        }

    def _env_from_registry(self, registry: DatasetRegistry):
        """Copy datasets into local env."""
        return dict(registry.datasets)

    def _repair_step(self, step, error, env):
        from ..agents.selfCritique import SelfCritiqueAgent
        import re
        agent = SelfCritiqueAgent(coder_model="qwen2.5:14b", max_loops=2)

        available_info = {name: list(df.columns) for name, df in env.items()}

        prompt = f"""
        You are an execution corrector.
        A function call failed during analysis.

        Failed step:
        {json.dumps(step)}

        Error:
        {str(error)}

        Available datasets and their columns:
        {json.dumps(available_info, indent=2)}

        Please rewrite ONLY this step so that it is executable,
        using valid column names and dataset references from the above list.
        Output a single corrected JSON list in the same 4-element format.
        """

        resp = agent._chat(prompt=prompt).strip()

        # --- 🧹 extract the JSON block first ---
        match = re.search(r"```json(.*?)```", resp, flags=re.S)
        if not match:
            match = re.search(r"\[(.*?)\]", resp, flags=re.S)
            if match:
                resp = "[" + match.group(1) + "]"
        else:
            resp = match.group(1).strip()

        # --- now parse safely ---
        try:
            fixed = safe_json_loads(resp)
            if isinstance(fixed, list) and len(fixed) == 4:
                print(f"🔧 Corrected step: {fixed}")
                return fixed
            else:
                print("⚠️ correction JSON malformed, skipping")
        except Exception as e:
            print(f"⚠️ Correction parse failed: {e}")

        return None

    def _execute_step(self, func, func_name, input_name, kwargs, env, results):
        """Executes a single operation (single or multi-input)."""
        # --- handle multi-input functions ---
        if isinstance(input_name, list):
            inputs = []
            for name in input_name:
                df = env.get(name) 
                if df is None:
                    df = results.get(name)
                if df is None:
                    raise KeyError(f"Input '{name}' not found in environment")
                inputs.append(df)
            return func(*inputs, **kwargs)
        else:
            df = env.get(input_name) 
            if df is None:
                df = results.get(input_name)
            if df is None:
                raise KeyError(f"Input '{input_name}' not found in environment")
            return func(df, **kwargs)


    def run_function_sequence(self, seq: str, registry: DatasetRegistry):
        """Main loop — executes or repairs each step."""
        try:
            ops = normalize_ops(safe_json_loads(seq))
        except Exception as e:
            raise ValueError(f"Invalid JSON sequence: {e}")

        env = self._env_from_registry(registry)
        results = {}
        last_result = None

        for step in ops:
            if not isinstance(step, list) or len(step) != 4:
                print(f"Bad step format, skipping: {step}")
                continue

            output_name, func_name, input_name, kwargs = step
            func = self.lib.get(func_name)
            if not func:
                print(f"Unknown function: {func_name}")
                continue

            try:
                result = self._execute_step(func, func_name, input_name, kwargs, env, results)
                print(f"Running {func_name} on '{input_name}' → '{output_name}'")
            except Exception as e:
                print(f"Error at step: {step}")
                print("Repairing early.")
                print("✅ Partial execution successfully.")
                return results  # stop immediately

            # --- success path ---
            results[output_name] = result
            env[output_name] = result
            if isinstance(result, pd.DataFrame):
                registry.datasets[output_name] = result
            last_result = result

        print("✅ Sequence executed successfully.")
        results["_FINAL_"] = last_result
        return results
