import json, ast, re
import ollama
from typing import Any, List, Dict

FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)

class Head2Planner:
    def __init__(self,
                 reasoning_model: str = "mistral-nemo:12b",
                 coder_model: str = "deepseek-coder:6.7b",
                 use_refiner: bool = True):
        self.reasoning_model = reasoning_model
        self.coder_model = coder_model
        self.use_refiner = use_refiner

    # ----------------- prompt -----------------
    def build_prompt(self, plan, dataset_meta) -> str:
        plan_text = plan if isinstance(plan, str) else "\n".join(str(x) for x in plan)
        meta_text = json.dumps(dataset_meta, indent=2, ensure_ascii=False)
        return f"""
        You are Head-2. Translate Head-1 PLAN into a JSON array of operations for a Python executor.

        DATASET META:
        {meta_text}

        HEAD-1 PLAN:
        {plan_text}

        Rules:
        - Output ONLY a JSON array. No prose, no headings.
        - If you compute relative years, resolve them to concrete integers.
        - Use keys like: step, op, input, columns, condition, agg, output, on, how.
        - Use only column names that exist in the provided META.
        """.strip()

    # ----------------- chat wrappers -----------------
    def _chat(self, model: str, content: str) -> str:
        r = ollama.chat(model=model, messages=[{"role": "user", "content": content}])
        return r.get("message", {}).get("content", "").strip()

    def _refine(self, raw_text: str) -> str:
        if not self.use_refiner or not self.coder_model:
            return raw_text
        prompt = f"""
        The text below should be a valid JSON array but may include extra words or fences.
        Return strictly the JSON only. If you see ```json fences, return only what's inside.

        Text:
        {raw_text}
        """
        return self._chat(self.coder_model, prompt)

    # ----------------- JSON extraction -----------------
    def _extract_json(self, text: str) -> str:
        # 1) try fenced block
        m = FENCE_RE.search(text)
        if m:
            candidate = m.group(1).strip()
            if candidate:
                return candidate

        # 2) try to find first JSON object/array by bracket matching
        start_idxs = [text.find("["), text.find("{")]
        start_idxs = [i for i in start_idxs if i != -1]
        if start_idxs:
            start = min(start_idxs)
            opening = text[start]
            closing = "]" if opening == "[" else "}"
            depth = 0
            for i, ch in enumerate(text[start:], start=start):
                if ch == opening:
                    depth += 1
                elif ch == closing:
                    depth -= 1
                    if depth == 0:
                        return text[start:i+1].strip()

        # 3) nothing obvious; return as-is (may be plain JSON already)
        return text.strip()

    # ----------------- last-ditch parse helpers -----------------
    def _loads_relaxed(self, s: str) -> Any:
        # first, strict JSON
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            pass

        # normalize common artifacts then try JSON again
        s2 = s.replace("\r", "").strip()
        # Remove leading "Here is ...", etc.
        if s2.lower().startswith("here"):
            s2 = self._extract_json(s2)
        # Drop trailing commas inside objects/arrays (simple, safe-ish)
        s2 = re.sub(r",(\s*[}\]])", r"\1", s2)
        try:
            return json.loads(s2)
        except json.JSONDecodeError:
            pass

        # fallback: try Python literal (if model returned dict syntax)
        # normalize true/false/null → True/False/None for literal_eval
        s3 = s2.replace("true", "True").replace("false", "False").replace("null", "None")
        try:
            return ast.literal_eval(s3)
        except Exception as e:
            print(f"unable to parse this... due to {e}")

    # ----------------- schema sanity -----------------
    def _validate_ops(self, ops: List[Dict], dataset_meta: List[Dict]) -> None:
        # build column map per dataset
        colmap = {d["name"]: set(d.get("columns", [])) for d in dataset_meta}
        for obj in ops:
            inp = obj.get("input")
            if inp in colmap:
                # check columns keys commonly used
                for key in ("columns",):
                    cols = obj.get(key)
                    if isinstance(cols, list):
                        bad = [c for c in cols if c not in colmap[inp]]
                        if bad:
                            raise ValueError(f"Unknown columns {bad} for dataset {inp}")
                # agg schema check (optional)
                agg = obj.get("agg")
                if isinstance(agg, dict):
                    cols = agg.get("columns")
                    if isinstance(cols, list):
                        bad = [c for c in cols if c not in colmap[inp]]
                        if bad:
                            raise ValueError(f"Unknown agg columns {bad} for dataset {inp}")

    # ----------------- main -----------------
    def plan(self, plan, dataset_meta):
        prompt = self.build_prompt(plan, dataset_meta)

        # 1) reasoning
        raw = self._chat(self.reasoning_model, prompt)
        print('############ RAW OUTPUT ############')
        print(raw)

        # 2) optional refine
        refined = self._refine(raw)
        print('############ REFINED OUTPUT ############')
        print(refined)
        # 3) extract the JSON payload robustly
        json_text = self._extract_json(refined)
        print('############ JSON OUTPUT ############')
        print(json_text)

        # 4) parse with relaxed loader
        ops = self._loads_relaxed(json_text)

        # 5) minimal schema sanity
        if isinstance(ops, list):
            self._validate_ops(ops, dataset_meta)
        else:
            raise ValueError("Head-2 expected a JSON array of operations")

        return ops


if __name__ == "__main__":
    dataset_meta = [{
        "name": "D1",
        "title": "Rainfall in sub-division and its departure from normal for Monsoon session from 1901-2021",
        "columns": ["subdivision","YEAR","JUN","JUL","AUG","SEP","JUN_SEP"]
    }]

    plan = [
        "1) D1[subdivision,YEAR,JUN,JUL,AUG,SEP,JUN_SEP] filter YEAR > (YEAR(now)-3)",
        "2) D1 group by YEAR compute mean(JUN_SEP)",
        "3) answer = trend(D1.mean_jun_sep)"
    ]

    head2 = Head2Planner()
    print("=== HEAD-2 OUTPUT ===")
    result = head2.plan(plan, dataset_meta)
    print(json.dumps(result, indent=2))
    from ..llm_tools.ollama_utils import OllamaManager
    OllamaManager.stop_model("deepseek-coder:6.7b")
    OllamaManager.stop_model("mistral-nemo:12b")