import os
import io
import json
import requests
import pandas as pd
from pathlib import Path

CACHE_DIR = Path("dataHandlers/data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

class DataFetcher:
    def __init__(self, cache_dir: Path = CACHE_DIR, timeout: int = 60):
        self.cache_dir = cache_dir
        self.timeout = timeout

    def _cache_path(self, url: str) -> Path:
        safe = url.replace("https://", "").replace("http://", "").replace("/", "_")
        return self.cache_dir / safe

    # ---------- generic dispatcher ----------
    def load(self, url: str, source_type: str = None):
        """Auto-detect the right loader if not specified."""
        breakpoint()
        if not source_type:
            if "api.data.gov.in" in url:
                source_type = "json_api"
            elif url.endswith(".csv"):
                source_type = "csv_static"
            elif url.endswith(".json"):
                source_type = "json_generic"
            elif "asmx" in url or "TokenNo=" in url:
                source_type = "token_json"
            else:
                source_type = "csv_generic"

        if source_type in ("csv_static", "csv_generic"):
            return self._load_csv(url)
        elif source_type in ("json_api", "json_generic"):
            return self._load_json(url)
        elif source_type == "token_json":
            return self._load_token_json(url)
        else:
            raise ValueError(f"Unknown source_type: {source_type}")


    # ---------- CSV ----------
    def _load_csv(self, url: str):
        path = self._download(url)
        try:
            df = pd.read_csv(path)
        except Exception:
            df = pd.read_csv(path, sep=";")
        df.columns = [c.strip() for c in df.columns]
        print(f"📊 Loaded CSV: {len(df)} rows × {len(df.columns)} cols")
        return df

    def _load_token_json(self, url):
        import pandas as pd
        import requests

        r = requests.get(url, timeout=60)
        r.raise_for_status()
        
        data = r.json()
        # Most .asmx endpoints wrap table data like {"Table": [ ... ]}
        if isinstance(data, dict) and "Table" in data:
            df = pd.DataFrame(data["Table"])
            print(f"📊 Loaded Token JSON: {len(df)} rows × {len(df.columns)} cols")
            return df

        # fallback if it's already a list
        if isinstance(data, list):
            df = pd.DataFrame(data)
            print(f"📊 Loaded Token JSON (list): {len(df)} rows × {len(df.columns)} cols")
            return df

        print("⚠️ Unknown JSON structure:", list(data.keys()))
        return data
    # ---------- JSON / API ----------
    def _load_json(self, url: str):
        # normalize ?format=json etc.
        if "format=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}format=json&limit=100"

        # append default API key if missing
        if "api-key=" not in url and "api.data.gov.in/resource" in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}api-key=579b464db66ec23bdd000001cdc3b564546246a772a26393094f5645"

        print(f"🔗 Fetching JSON API: {url}")
        r = requests.get(url, timeout=self.timeout)
        r.raise_for_status()

        # print(r.headers.get("Content-Type", ""))
        if "text/csv" in r.headers.get("Content-Type", ""):
            # Fallback to CSV reader if API sent CSV instead of JSON
            from io import StringIO
            df = pd.read_csv(StringIO(r.text))
            print(f"📊 Loaded CSV from API: {len(df)} rows × {len(df.columns)} cols")
            return df

        data = r.json()
        if isinstance(data, dict) and "records" in data:
            df = pd.DataFrame(data["records"])
        else:
            df = pd.json_normalize(data)
        print(f"📊 Loaded JSON: {len(df)} rows × {len(df.columns)} cols")
        return df

    def load_market_price_data(self, state, district=None, commodity=None, date=None, limit=100):
        rid = "35985678-0d79-46b4-9ed6-6f13308a1d24"
        base = f"https://api.data.gov.in/resource/{rid}"
        params = {
            "api-key": "579b464db66ec23bdd000001cdc3b564546246a772a26393094f5645",
            "format": "json",
            "limit": limit,
        }
        if state:
            params["filters[State]"] = state
        if district:
            params["filters[District]"] = district
        if commodity:
            params["filters[Commodity]"] = commodity
        if date:
            params["filters[Arrival_Date]"] = date

        print("🔗 Fetching filtered market price data...")
        r = requests.get(base, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data.get("records", []))
        print(f"📊 {len(df)} rows × {len(df.columns)} cols")
        return df

    def get_download_url(resource_id: str, fmt="json"):
        url = "https://www.data.gov.in/backend/dms/v1/ogdp/resource/file/export"
        payload = {
            "resource_id": resource_id,
            "file_format": fmt,
            "export_status": "download"
        }
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("download_url")
    # ---------- helper ----------
    def _download(self, url: str) -> Path:
        path = self._cache_path(url)
        if path.exists():
            return path
        print(f"⬇️  Downloading {url}")
        r = requests.get(url, timeout=self.timeout)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)
        return path
