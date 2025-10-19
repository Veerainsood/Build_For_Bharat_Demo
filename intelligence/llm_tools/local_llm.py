# intelligence/llm_tools/local_llm.py
import requests, json

class LocalLLM:
    def __init__(self, model: str = "phi3:mini", host: str = "http://localhost:11434"):
        self.model = model
        self.host = host.rstrip("/")

    def chat(self, prompt: str, temperature: float = 0.0, timeout: int = 120) -> str:
        """
        Calls Ollama's /api/generate (non-OpenAI). Returns full text.
        """
        url = f"{self.host}/api/generate"
        payload = {"model": self.model, "prompt": prompt, "options": {"temperature": temperature}}
        with requests.post(url, json=payload, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            text = ""
            for line in r.iter_lines():
                if not line:
                    continue
                obj = json.loads(line.decode("utf-8"))
                text += obj.get("response", "")
            return text.strip()
