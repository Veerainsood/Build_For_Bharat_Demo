import requests

class OllamaManager:
    BASE_URL = "http://localhost:11434/api"

    @staticmethod
    def stop_all():
        """Stop all running models (safe global unload)."""
        try:
            requests.post(f"{OllamaManager.BASE_URL}/stop", timeout=5)
            print("🧹 All Ollama models stopped.")
        except Exception as e:
            print(f"⚠️ Could not stop models: {e}")

    @staticmethod
    
    def stop_model(model_name: str):
        """Force stop a running Ollama model via CLI."""
        import subprocess
        try:
            subprocess.run(
                ["ollama", "stop", model_name],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            print(f"🧹 Force-stopped Ollama model: {model_name}")
        except Exception as e:
            print(f"⚠️ Failed to stop {model_name}: {e}")
