# intelligence/llm_tools/pmkisan_selector.py
import json, re, numpy as np
from sentence_transformers import SentenceTransformer
from .local_llm import LocalLLM
import os
import ollama

class PMKisanSelector:
    """
    Handles hierarchical selection for PM-KISAN datasets.
    Stage 2b: select STATE first, then subselect district/year/installment.
    """

    def __init__(self, root_json, model="mistral-nemo:12b", cache_dir="intelligence/embedding_cache/"):
        self.root_json = root_json
        # self.llm = LocalLLM(model=model)
        self.model = model
        self.sentence_model = SentenceTransformer(
            "./models/all-MiniLM-L6-v2",
            device="cpu",  # or "cuda" if GPU is available
            local_files_only=True
        )
        self.family_name = "Beneficiaries_(PM_KISAN)"
        self.cache_file = os.path.join(cache_dir, f"{self.family_name}.npz")
        self.states = list(root_json["states"].keys())
        self._recompute_and_save()

    # ----------------------------------------------------------------
    def _cache_valid(self):
        try:
            cached = np.load(self.cache_file, allow_pickle=True)
            cached_states = list(cached["states"])
            return cached_states == self.states
        except Exception:
            return False

    # ----------------------------------------------------------------
    def _recompute_and_save(self):
        self.state_vecs = self.sentence_model.encode(self.states, normalize_embeddings=True)
        np.savez(self.cache_file , vecs=self.state_vecs , states=self.states)
        print(f"💾 Saved new cache: {self.cache_file}")



    # --- Stage 1: state-level selection ---
    def select_state(self, query, threshold=0.35):
        qvec = self.sentence_model.encode([query], normalize_embeddings=True)[0]
        sims = np.dot(self.state_vecs, qvec)
        idx = np.argmax(sims)
        if sims[idx] < threshold:
            """
            Let LLM pick the most relevant state index from the list.
            Returns: {"state": <state_name>} or {"state": -1}
            """

            # Construct numbered state list
            state_list_text = "\n".join([f"{i+1}. {s}" for i, s in enumerate(self.states)])

            prompt = f"""
            You are a classification assistant.
            Pick the most relevant STATE INDEX from the list below based on the user query.
            - Only return the number.
            - If no clear match, return -1.

            STATES:
            {state_list_text}

            Example:
            Q: beneficiaries in Guntur district -> 26
            Q: farmers in Ladakh -> 32
            Q: rainfall in Delhi -> 12
            Q: crops in Nicobar district -> 33

            User query: "{query}"
            Answer:
            """

            try:
                resp = ollama.chat(
                    model="qwen2.5:7b",
                    messages=[
                        {"role": "system", "content": "Return only a number."},
                        {"role": "user", "content": prompt}
                    ],
                ).get("message", {}).get("content", "").strip()

                # Extract numbers safely
                nums = re.findall(r"\d+", resp)
                if not nums:
                    return {"state": self.states[0]}

                idx = int(nums[0]) - 1
                if idx < 0 or idx >= len(self.states):
                    return {"state": self.states[0]}

                return {"state": self.states[idx]}

            except Exception as e:
                return {"state": self.states[0]}

        return {"state": self.states[idx]}

    # --- Stage 2: district/year/instalment selection within state ---
    def select_subfile(self, query, state):
        # --- locate files ---
        path = self.root_json["states"][state]
        jsn = path[-5:]
        file = path[:-5] + "_urls"
        path_url = file + jsn

        # --- load data ---
        subdata = json.load(open(path))         # {'PM-KISAN': [list of district/year strings]}
        urls_data = json.load(open(path_url))   # {'PM-KISAN': {<entry>: [url, url, ...]}}
        urls = next(iter(urls_data.values()))   # inner dict of URL lists

        # --- get the real entries ---
        keys = next(iter(subdata.values()))     # list of "Nicobars:2022-23[11th,12th,13th]" etc.

        # --- compute similarity ---
        vecs = self.sentence_model.encode(keys, normalize_embeddings=True)
        qvec = self.sentence_model.encode([query], normalize_embeddings=True)[0]
        sims = np.dot(vecs, qvec)
        top_idx = np.argsort(-sims)[:5]

        # --- hybrid logic: direct or LLM refinement ---
        if len(top_idx) > 1 and sims[top_idx[0]] - sims[top_idx[1]] > 0.08 and sims[top_idx[0]] > 0.4:
            best_idx = top_idx[0]
        else:
            # fallback: LLM re-ranking among top-5
            candidates = "\n".join(f"{i+1}. {keys[idx]}" for i, idx in enumerate(top_idx))
            prompt = f"""
            The user asked: "{query}"
            Choose which of the following district/year entries best fits this query.
            Respond ONLY with the number (e.g., 2 or 4). No explanations.

            {candidates}
            """
            # breakpoint()
            raw = ollama.chat(model=self.model, messages=[{"role": "user", "content": prompt}]).get("message", {}).get("content", "").strip()
            # breakpoint()
            import re
            nums = [abs(int(n)) for n in re.findall(r"\d+", raw)]
            best_idx = top_idx[nums[0]-1] if nums else top_idx[0]

        # --- construct result ---
        key = keys[best_idx]
        return {
            "state": state,
            "entry": key,
            "file_path": urls.get(key, []),  # safe dictionary access
        }


    def select(self, query):
        # breakpoint()
        state_res = self.select_state(query)
        if state_res["state"] == -1:
            return {"selected_files": [-1]}

        sub_res = self.select_subfile(query, state_res["state"])
        return {"selected_files": [sub_res]}
