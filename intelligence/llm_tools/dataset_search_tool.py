# intelligence/llm_tools/dataset_search_tool.py
import json
import re
import numpy as np
from sentence_transformers import SentenceTransformer
from .local_llm import LocalLLM
import os
import ollama

class DatasetSearchTool:
    """
    Stage-1 selector for Build for Bharat.
    Chooses dataset FAMILY from sector_index.json.
    Returns: {"selected_datasets": ["<family>"]} or {"selected_datasets": [-1]}.
    """

    def __init__(self, dataset_map: dict, model: str = "mistral-nemo:12b"):
        self.dataset_map = dataset_map
        self.model = model
        # self.llm = LocalLLM(model=model)
        self.dataset_ids = {i + 1: name for i, name in enumerate(dataset_map.keys())}

        # ---- Aliases for rule-based and embedding cues ----
        self.aliases = {
            "Beneficiaries (PM-KISAN)": ["farmers", "pm kisan", "beneficiaries"],
            "Agricultural Marketing": ["mandi", "market price", "commodity rates"],
            "Crop Development & Seed Production": ["crop varieties", "seed", "yield"],
            "Research, Education & Biotechnology": ["agriculture research", "biotech"],
            "Temperature and Rainfall": ["rainfall", "temperature", "climate", "weather"],
        }

        # ---- SentenceTransformer (CPU-only, pre-loaded once) ----
        try:
            model_path = "./models/all-MiniLM-L6-v2"
            if not os.path.exists(model_path):
                raise FileNotFoundError

            self.sentence_model = SentenceTransformer(
                model_path,
                device="cpu",
                local_files_only=True
            )
        except Exception:
            print("⚠️ Local embedding model not found. Downloading...")
            self.sentence_model = SentenceTransformer("all-MiniLM-L6-v2" , device="cpu")
            os.makedirs("./models", exist_ok=True)
            self.sentence_model.save(model_path)
            self.sentence_model = SentenceTransformer(
                model_path,
                device="cpu",
                local_files_only=True
            )
            print("✅ Model cached locally.")

        dataset_texts = [
            "Beneficiaries (PM-KISAN): farmers, government benefits, instalments, village-wise data",
            "Agricultural Marketing: market price, commodity rates, mandi",
            "Crop Development & Seed Production: crop varieties, yield, production, seeds",
            "Research, Education & Biotechnology: agriculture research, innovation, biotechnology",
            "Temperature and Rainfall: climate, rainfall, weather, temperature",
        ]
        self.dataset_texts = dataset_texts
        self.dataset_vecs = self.sentence_model.encode(dataset_texts, normalize_embeddings=True)

    # ---- Semantic retriever first ----
    def retrieve_relevant_families(self, query, top_k=2, threshold=0.45):
        qvec = self.sentence_model.encode([query], normalize_embeddings=True)[0]
        sims = np.dot(self.dataset_vecs, qvec)
        top_indices = np.argsort(-sims)[:top_k]
        best, second = sims[top_indices[0]], sims[top_indices[1]]
        selected = [self.dataset_texts[i].split(":")[0] for i in top_indices if sims[i] > threshold]
        # for name, score in zip(self.dataset_texts, sims):
        #     print(f"{name.split(':')[0]:40s}  {score:.3f}")
        # dynamic threshold: if both scores are close (within 0.1), keep both
        if abs(best - second) < 0.1 and second > 0.35:
            selected = [self.dataset_texts[i].split(":")[0] for i in top_indices[:2]]
        return selected or [-1]

    # ---- Main selector ----
    def select(self, query: str):
        # selected = self.retrieve_relevant_families(query)

        # # if embeddings find strong match(es), skip LLM
        # if selected != [-1]:
        #     return {"selected_datasets": selected}

        # else fallback to LLM reasoning
        alias_lines = "\n".join(
            f"{i}. {name} — keywords: {', '.join(keywords)}"
            for i, (name, keywords) in enumerate(self.aliases.items(), start=1)
        )

        prompt = f"""
        SYSTEM: You are a strict dataset selector. 
        Your job is classification, not answering the question.

        INSTRUCTIONS:
        - Output ONLY dataset IDs.
        - NO text, NO explanation, NO punctuation.
        - If unsure, output -1.
        - If multiple topics, output all relevant IDs.

        DATASETS:
        {alias_lines}

        Examples:
        Q: "rainfall effect on rice yield"  -> 5 3
        Q: "number of PM Kisan beneficiaries in UP" -> 9
        Q: "sugarcane production by district in Maharashtra" -> 3
        Q: "compare crop data Maharashtra vs Karnataka" -> 3
        Q: "education scheme growth in Bihar" -> -1

        User query: "{query}"
        Answer:
        """
        # breakpoint()
        raw = raw = ollama.chat(
            model=self.model,
            messages=[{"role": "system", "content": "Return ONLY numbers. No text."},
                    {"role": "user", "content": prompt}]
        ).get("message", {}).get("content", "").strip()
        # breakpoint()
        ids = self._extract_numbers(raw)
        valid = [self.dataset_ids[i] for i in ids if i in self.dataset_ids]
        if not valid:
            return {"selected_datasets": [-1]}
        return {"selected_datasets": valid}

    # ---- utils ----
    def _extract_numbers(self, text: str) -> list[int]:
        nums = re.findall(r"-?\d+", text)
        return [abs(int(n)) for n in nums] if nums else [-1]
