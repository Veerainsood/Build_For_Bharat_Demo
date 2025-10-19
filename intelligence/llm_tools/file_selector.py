import json, re
from .local_llm import LocalLLM

class FileSelector:
    """
    LLM #2: pick files within a dataset family.
    Returns: {"selected_files": ["<file_id>", ...]} or {"selected_files": [-1]}
    """

    def __init__(self, model: str = "phi3:mini"):
        self.llm = LocalLLM(model=model)

    def _extract_numbers(self, text: str) -> list[int]:
        nums = re.findall(r"-?\d+", text.strip())
        if not nums: return []
        return [abs(int(n)) for n in nums]  # clip negatives

    def _prefilter(self, files: list[dict], query: str, top_k: int = 25) -> list[dict]:
        """
        Fast pre-filter by keyword overlap to keep context tiny.
        Each file = {"id": "...", "title": "..."} (as in your family index).
        """
        q = query.lower()
        # light keyword bag from the query
        qterms = set(re.findall(r"[a-z]+", q))
        def score(title: str) -> int:
            words = set(re.findall(r"[a-z]+", title.lower()))
            return len(qterms & words)
        ranked = sorted(files, key=lambda f: score(f.get("title","")), reverse=True)
        return ranked[:top_k] if ranked else files[:top_k]

    def select_files(self, family_name: str, files: list[dict], query: str, allow_multi: bool = True) -> dict:
        # 1) prefilter + build compact numbered list
        cand = self._prefilter(files, query, top_k=25)
        if not cand:
            return {"selected_files": [-1]}

        numbered = []
        idx_to_id = {}
        for i, f in enumerate(cand, start=1):
            idx_to_id[i] = f["id"]
            title = f.get("title", f["id"])
            numbered.append(f"{i}. {title}")

        numbered_block = "\n".join(numbered)

        # 2) ask model for just numbers (no JSON)
        plural = "IDs" if allow_multi else "a single ID"
        rule_multi = "- If more than one file is clearly needed, include multiple IDs separated by commas." if allow_multi else "- Choose exactly one ID."
        prompt = f"""
            You are a file selector for the dataset family "{family_name}".
            Select {plural} of the files below that best answer the user query.

            FILES:
            {numbered_block}

            QUERY:
            "{query}"

            RESPONSE RULES:
            - Respond ONLY with integers (e.g., 4 or 2,5,7). No words, no JSON, no punctuation.
            {rule_multi}
            - Respond -1 if none match well.
            """

        raw = self.llm.chat(prompt, temperature=0)
        ids = self._extract_numbers(raw)

        if not ids or ids == [-1]:
            return {"selected_files": [-1]}

        # 3) map indices → file IDs (ignore out-of-range)
        picked = [idx_to_id[i] for i in ids if i in idx_to_id]
        return {"selected_files": picked or [-1]}
