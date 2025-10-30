# intelligence/llm_tools/local_llm.py
import ollama

class LocalLLM:
    def __init__(self, model: str = "qwen2.5:14b", host: str = "http://localhost:11434"):
        self.model = model
        self.host = host.rstrip("/")

    def chat(self, prompt: str, temperature: float = 0.0, timeout: int = 2000) -> str:
        """
        Calls Ollama's /api/generate (non-OpenAI). Returns full text.
        """
        
