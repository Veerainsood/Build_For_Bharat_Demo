# intelligence/llm_tools/file_search_tool.py
import re
import numpy as np
from sentence_transformers import SentenceTransformer
from .local_llm import LocalLLM


class FileSearchTool:
    """
    Stage-2 selector for Build for Bharat.

    Given a dataset family (e.g. "Temperature and Rainfall") and a user query,
    retrieves top-k candidate files using hybrid (semantic + heuristic) scoring,
    then refines the ranking via a local LLM to pick the most relevant few.
    """

    def __init__(self, family_index: dict, model="mistral-nemo:12b", cache_dir="intelligence/embedding_cache/"):
        # unwrap: {"Temperature and Rainfall": [ {...}, {...} ]}
        # breakpoint()
        if isinstance(family_index, dict):
            self.family_name, entries = next(iter(family_index.items()))
        else:
            self.family_name, entries = "unknown", family_index

        self.family_index = entries  # list[{id,title,index}]
        self.llm = LocalLLM(model=model)
        self.sentence_model = SentenceTransformer("./models/bge-base-en-v1.5", device="cpu", local_files_only=True)

        self.cache_path = cache_dir
        # simple state/UT list for boosting
        self.region_aliases = {
            "Andhra Pradesh": ["South Peninsula", "Southern Peninsula", "Peninsular India"],
            "Arunachal Pradesh": ["North East India", "NE India"],
            "Assam": ["North East India", "NE India"],
            "Bihar": ["North East India", "NE India"],
            "Chhattisgarh": ["Central India"],
            "Goa": ["South Peninsula", "Southern Peninsula", "Peninsular India"],
            "Gujarat": ["North West India", "NW India"],
            "Haryana": ["North West India", "NW India"],
            "Himachal Pradesh": ["North West India", "NW India"],
            "Jharkhand": ["East India", "Central India"],
            "Karnataka": ["South Peninsula", "Southern Peninsula", "Peninsular India"],
            "Kerala": ["South Peninsula", "Southern Peninsula", "Peninsular India"],
            "Madhya Pradesh": ["Central India"],
            "Maharashtra": ["Central India", "South Peninsula"],
            "Manipur": ["North East India", "NE India"],
            "Meghalaya": ["North East India", "NE India"],
            "Mizoram": ["North East India", "NE India"],
            "Nagaland": ["North East India", "NE India"],
            "Odisha": ["East India", "Central India"],
            "Punjab": ["North West India", "NW India"],
            "Rajasthan": ["North West India", "NW India"],
            "Sikkim": ["North East India", "NE India"],
            "Tamil Nadu": ["South Peninsula", "Southern Peninsula", "Peninsular India"],
            "Telangana": ["South Peninsula", "Southern Peninsula", "Peninsular India"],
            "Tripura": ["North East India", "NE India"],
            "Uttar Pradesh": ["North India", "Central India"],
            "Uttarakhand": ["North India", "North West India"],
            "West Bengal": ["East India", "North East India"],
            "Delhi": ["North India", "North West India"],
            "Jammu & Kashmir": ["North India", "North West India"],
            "Ladakh": ["North India", "North West India"],
            "Puducherry": ["South Peninsula", "Southern Peninsula"],
            "Andaman & Nicobar Islands": ["South Peninsula", "Bay of Bengal", "Island regions"],
            "Chandigarh": ["North India", "North West India"],
            "Lakshadweep": ["South Peninsula", "Arabian Sea", "Island regions"],
            "Dadra and Nagar Haveli and Daman and Diu": ["West Coast", "North West India"]
        }
        import os

        if os.path.exists(self.cache_path):
            try:
                cached = np.load(self.cache_path + self.family_name + ".npz", allow_pickle=True)
                self.titles = list(cached["titles"])
                if self._cache_valid():
                    self.vecs = cached["vecs"]
                    print(f"✅ Loaded cached embeddings for {self.family_name}")
                else:
                    print(f"⚠️ Cache outdated for {self.family_name} — recomputing...")
                    self._recompute_and_save()
            except Exception as e:
                print(f"⚠️ Cache read error for {self.family_name}: {e}. Recomputing...")
                self._recompute_and_save()
        else:
            print(f"🆕 No cache found for {self.family_name}, computing embeddings...")
            self._recompute_and_save()

    # ----------------------------------------------------------------
    def _cache_valid(self):
        current_titles = [d["title"] for d in self.family_index]
        return getattr(self, "titles", []) == current_titles

    # ----------------------------------------------------------------
    def _recompute_and_save(self):
        self.titles = [d["title"] for d in self.family_index]
        self.vecs = self.sentence_model.encode(self.titles, normalize_embeddings=True)
        np.savez(self.cache_path + self.family_name, vecs=self.vecs, titles=self.titles)
        print(f"💾 Saved new cache: {self.cache_path}")

    # ------------------------------------------------------------------
    def _apply_boosts(self, query: str, title: str, base_score: float) -> float:
        
        import datetime

        score = base_score
        current_year = datetime.datetime.now().year

        # temporal cues: reward proximity
        years = re.findall(r"(19|20)\d{2}", query)
        for y in years:
            try:
                y_int = int(y)
                delta = max(0, min(10, current_year - y_int))
                score += max(0, 0.15 - 0.015 * delta)
            except ValueError:
                pass

        # 'last N years' handling
        if m := re.search(r"last\s+(\d+)\s+year", query):
            window = int(m.group(1))
            for y in re.findall(r"(19|20)\d{2}", title):
                if current_year - int(y) <= window:
                    score += 0.1

        # recent/last keyword fallback
        if "recent" in query or "last" in query:
            score += 0.03

        # geographic matches
        for state, aliases in self.region_aliases.items():
            if state.lower() in query.lower():
                for alias in aliases:
                    if alias.lower() in title.lower():
                        score += 0.18


        return min(score, 1.0)


    # ------------------------------------------------------------------
    def retrieve(self, query: str, top_k: int = 10, threshold: float = 0.35):
        """Hybrid semantic + boosted recall."""
        qvec = self.sentence_model.encode([query], normalize_embeddings=True)[0]
        sims = np.dot(self.vecs, qvec)
        sims = [self._apply_boosts(query, t, float(s)) for t, s in zip(self.titles, sims)]
        top_idx = np.argsort(-np.array(sims))[:top_k]

        selected = [
            {"index": self.family_index[i]["index"], "title": self.family_index[i]["title"], "score": float(sims[i])}
            for i in top_idx if sims[i] >= threshold
        ]
        return selected or [{"index": -1}]


    # ------------------------------------------------------------------
    def select(self, query: str):
        """Main entry: hybrid retrieval → LLM re-ranking."""
        # breakpoint()
        results = self.retrieve(query)
        idx_map = {d['index']: i for i, d in enumerate(self.family_index)}
        if not results or results[0]["index"] == -1:
            # fallback full-reasoning on all titles
            titles_text = "\n".join(f"{d['index']}. {d['title']}" for i, d in enumerate(self.family_index))
            prompt = f"""
            You are a dataset file selector. Choose which of the following datasets
            best answer the user's query.

            DATASETS:
            {titles_text}

            USER QUERY:
            "{query}"

            Respond ONLY with the most relevant dataset number(s), separated by commas.
            Return -1 if none match.
            """
            raw = self.llm.chat(prompt, temperature=0)
            nums = [int(n) for n in re.findall(r"\d+", raw)] or [-1]
            selected = [
                i for i in nums if 1 <= i
            ]
            return {"selected_indexes": selected or [{"id": -1}]}

        # -------------------- LLM re-ranking --------------------
        # top_titles = [r["title"] for r in results]
        titles_text = "\n".join(f"{t['index']}. {t['title']}" for i, t in enumerate(results))
        prompt = f"""
            Rank which of the following datasets most directly answer:
            "{query}"

            Note:
            - Treat "Southern Peninsula" or "Peninsular India" as covering Tamil Nadu, Kerala, Andhra Pradesh, Telangana, and Karnataka.
            - Prefer datasets mentioning recent years or regional specificity.

            DATASETS:
            {titles_text}

            Respond ONLY with the top 3 numbers (e.g., 2,5,1) in relevance order.
            If unsure, include fewer. No explanations. Choose at least 1.
        """

        raw = self.llm.chat(prompt, temperature=0)
        nums = [int(n) for n in re.findall(r"\d+", raw)]

        # reattach metadata after reranking
        # breakpoint()
        reranked = [
            {'index':i,'title':self.family_index[idx_map[i]]['title']}
            for i in nums if 1 <= i 
        ]

        return {"selected_files": reranked[:3] if reranked else [{'index':r["index"],'title':r["title"]} for r in results[:3]]}

