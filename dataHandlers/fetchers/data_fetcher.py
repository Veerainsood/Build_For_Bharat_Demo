import os
import io
import json
import requests
import pandas as pd
from pathlib import Path
from xml.etree import ElementTree

from dotenv import load_dotenv
load_dotenv()


CACHE_DIR = Path("dataHandlers/data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


class DataFetcher:
    
    LOCAL_FAMILIES = {
            "Crop Development & Seed Production",
            "Temperature and Rainfall",
            "Research, Education & Biotechnology"
        }

    BASE_DIR = Path("dataHandlers/data")

    def __init__(self, cache_dir: Path = CACHE_DIR, timeout: int = 60):
        self.cache_dir = cache_dir
        self.timeout = timeout

    def _cache_path(self, url: str) -> Path:
        safe = url.replace("https://", "").replace("http://", "").replace("/", "_")
        return self.cache_dir / safe

    def load_any(self, family_name ,entry):
        """
        entry can be:
          - {'id': <url>, 'title': ...}
          - {'file_path': [<urls>...], 'state': ..., 'entry': ...}   (PM-KISAN)
          - a plain string URL
        returns: pandas.DataFrame
        """
        # breakpoint()
        if isinstance(entry, str):
            return self.load(entry)

        if isinstance(entry,dict) and 'index' in entry.keys():
            if family_name in self.LOCAL_FAMILIES:
            
                folder = self.BASE_DIR / family_name
            
                if not folder.exists():
                    raise FileNotFoundError(f"No such folder: {folder}")

                # find file like 0.csv, 0.xlsx, etc.

                matches = list(folder.glob(f"{entry['index']}.*"))
                if not matches:
                    raise FileNotFoundError(f"No file found for index {entry} in {family_name}")

                file_path = matches[0]
                ext = file_path.suffix.lower()

                print(f"📁 Using local dataset: {file_path.name}")

                if ext in [".csv", ".txt"]:
                    df = pd.read_csv(file_path)
                elif ext in [".xls", ".xlsx"]:
                    df = pd.read_excel(file_path)
                elif ext == ".json":
                    df = pd.read_json(file_path)
                elif ext == ".xml":
                    df = self._load_xml(file_path)
                else:
                    raise ValueError(f"Unsupported local file type: {ext}")

                print(f"📊 Loaded local file: {len(df)} rows × {len(df.columns)} cols")
                return df
                
            else:
                raise ValueError(f"Family '{entry}' does not support local indexing.")
                
            
        # handle PM-KISAN style (list of URLs)
        if "file_path" in entry and isinstance(entry["file_path"], list):
            key = entry.get("entry") or entry["state"]
            url_map = {key: entry["file_path"]}
            return self.load_pmkisan_family(key, url_map)


        # handle normal case with 'id'
        url = entry.get("id")
        if url:
            return self.load(url)

        raise ValueError(f"Unrecognized entry format: {entry}")

    # ---------- main dispatcher ----------
    def load(self, url: str, source_type: str = None):
        if not source_type:
            url_l = url.lower()
            if "api.data.gov.in" in url_l:
                source_type = "json_api"
            elif "asmx" in url_l or "tokenno=" in url_l:
                source_type = "token_json"
            elif url_l.endswith((".csv", ".txt")):
                source_type = "csv_static"
            elif url_l.endswith((".xls", ".xlsx")):
                source_type = "excel_static"
            elif url_l.endswith(".json"):
                source_type = "json_generic"
            elif url_l.endswith(".xml") or "xml" in url_l:
                source_type = "xml_generic"
            else:
                source_type = "csv_generic"

        if source_type in ("csv_static", "csv_generic"):
            return self._load_csv(url)
        elif source_type == "excel_static":
            return self._load_excel(url)
        elif source_type in ("json_api", "json_generic"):
            return self._load_json(url)
        elif source_type == "token_json":
            return self._load_token_json(url)
        elif source_type == "xml_generic":
            return self._load_xml(url)
        else:
            raise ValueError(f"Unknown source_type: {source_type}")

    def load_pmkisan_family(self, key: str, url_map: dict):
        """
        key → e.g. 'Nicobars:2022-23[11th,12th,13th]'
        url_map → dict loaded from <state>_urls.json
        merges all installments' CSV/JSON responses into one df
        """
        if key not in url_map:
            raise KeyError(f"No URL mapping found for {key}")

        urls = url_map[key]
        if not isinstance(urls, list):
            urls = [urls]

        dfs = []
        for u in urls:
            try:
                df = self._load_token_json(u)
                if df is not None and not df.empty:
                    dfs.append(df)
            except Exception as e:
                print(f"⚠️ {u} failed: {e}")

        if dfs:
            merged = pd.concat(dfs, ignore_index=True)
            print(f"✅ Merged {len(dfs)} installments → {len(merged)} rows total")
            return merged
        else:
            print(f"⚠️ No data fetched for {key}")
            return pd.DataFrame()


    # ---------- CSV ----------
    def _load_csv(self, url_or_path):
        path = url_or_path if isinstance(url_or_path, Path) else self._download(url_or_path)
        import csv, pandas as pd

        encodings = ["utf-8-sig", "utf-8", "latin-1", "windows-1252"]
        seps = [",", ";", "\t", "|"]

        # 1️⃣ Try a few encoding/sep combinations quickly
        for enc in encodings:
            for sep in seps:
                try:
                    df = pd.read_csv(path, encoding=enc, sep=sep, engine="python", on_bad_lines="skip")
                    if df.shape[1] > 1:  # at least looks tabular
                        print(f"📊 Loaded CSV ({enc}, '{sep}') → {len(df)}×{len(df.columns)}")
                        return df
                except Exception:
                    continue

        # 2️⃣ Sniff delimiter as last resort
        try:
            with open(path, "r", errors="replace", newline="") as f:
                sample = f.read(4096)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    sep = dialect.delimiter
                except csv.Error:
                    sep = ","
                f.seek(0)
                df = pd.read_csv(f, encoding="latin-1", sep=sep, engine="python", on_bad_lines="skip")
                print(f"📊 Loaded CSV (sniffed '{sep}') → {len(df)}×{len(df.columns)}")
                return df
        except Exception as e:
            print(f"❌ Could not parse CSV {path}: {e}")

        # 3️⃣ Fallback: treat as text and split roughly
        with open(path, "r", errors="replace") as f:
            lines = [l.strip().split(",") for l in f.readlines()]
        df = pd.DataFrame(lines)
        print(f"⚠️ Rough CSV parse for {path}, shape={df.shape}")
        return df

    
    # ---------- Excel ----------
    def _load_excel(self, path_or_url: str):
        path = path_or_url if isinstance(path_or_url, Path) else self._download(path_or_url)
        p = Path(path)
        try:
            df = pd.read_excel(p)
            print(f"📊 Loaded Excel: {len(df)} rows × {len(df.columns)} cols")
            return df
        except Exception as e:
            print(f"❌ Excel load failed for {p}: {e}")
            return None
    
    # ---------- JSON / API ----------
    def _load_json(self, url: str):
        if "format=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}format=json&limit=100"

        if "api-key=" not in url and "api.data.gov.in/resource" in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}api-key=579b464db66ec23bdd000001cdc3b564546246a772a26393094f5645"

        print(f"🔗 Fetching JSON: {url}")
        r = requests.get(url, timeout=self.timeout)
        r.raise_for_status()

        ctype = r.headers.get("Content-Type", "")
        if "csv" in ctype:
            from io import StringIO
            return pd.read_csv(StringIO(r.text))

        try:
            data = r.json()
        except json.JSONDecodeError:
            print("⚠️ Response not JSON, attempting CSV fallback")
            return pd.read_csv(io.StringIO(r.text))

        if isinstance(data, dict):
            if "records" in data:
                df = pd.DataFrame(data["records"])
            elif "Table" in data:
                df = pd.DataFrame(data["Table"])
            else:
                df = pd.json_normalize(data)
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            print("⚠️ Unknown JSON structure")
            df = pd.DataFrame()

        print(f"📊 Loaded JSON: {len(df)} rows × {len(df.columns)} cols")
        return df

    # ---------- PM-KISAN / .asmx ----------
    def _load_token_json(self, url: str):
        r = requests.get(url, timeout=self.timeout)
        r.raise_for_status()
        text = r.text.strip()

        # handle XML-wrapped responses (often returned by .asmx)
        if text.startswith("<"):
            try:
                root = ElementTree.fromstring(text)
                json_text = root.text or ""
                data = json.loads(json_text)
            except Exception:
                print("⚠️ Could not parse XML → JSON structure.")
                return None
        else:
            data = r.json()

        if isinstance(data, dict) and "Table" in data:
            df = pd.DataFrame(data["Table"])
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            print("⚠️ Unexpected token JSON structure.")
            df = pd.json_normalize(data)

        print(f"📊 Loaded Token JSON: {len(df)} rows × {len(df.columns)} cols")
        return df

    # ---------- XML ----------
    def _load_xml(self, url: str):
        print(f"🔗 Fetching XML: {url}")
        r = requests.get(url, timeout=self.timeout)
        r.raise_for_status()
        try:
            root = ElementTree.fromstring(r.text)
            data = [{child.tag: child.text for child in elem} for elem in root]
            df = pd.DataFrame(data)
            print(f"📊 Loaded XML: {len(df)} rows × {len(df.columns)} cols")
            return df
        except Exception as e:
            print(f"❌ XML parse failed: {e}")
            return None

    # ---------- market price API ----------
    def load_market_price_data(self, state, district=None, commodity=None, date=None, limit=100):
        rid = "35985678-0d79-46b4-9ed6-6f13308a1d24"
        base = f"https://api.data.gov.in/resource/{rid}"
        params = {
            "api-key": "579b464db66ec23bdd000001cdc3b564546246a772a26393094f5645",
            "format": "json",
            "limit": limit,
        }
        if state: params["filters[State]"] = state
        if district: params["filters[District]"] = district
        if commodity: params["filters[Commodity]"] = commodity
        if date: params["filters[Arrival_Date]"] = date

        print("🔗 Fetching filtered market price data...")
        r = requests.get(base, params=params, timeout=60)
        r.raise_for_status()
        df = pd.DataFrame(r.json().get("records", []))
        print(f"📊 {len(df)} rows × {len(df.columns)} cols")
        return df

    # ---------- file downloader ----------
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
