# tools/bootstrap_market_filters.py
import requests, pandas as pd, json
from tqdm import tqdm

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"
API_KEY = "579b464db66ec23bdd000001cdc3b564546246a772a26393094f5645"

def sample_values(limit_per_page=500, max_pages=40):
    values = {k: set() for k in ["State","District","Market","Commodity","Variety","Grade"]}
    offset = 0
    for _ in tqdm(range(max_pages)):
        url = f"https://api.data.gov.in/resource/{RESOURCE_ID}?api-key={API_KEY}&format=json&limit={limit_per_page}&offset={offset}"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json().get("records", [])
        if not data:
            break
        df = pd.DataFrame(data)
        for col in values:
            if col in df.columns:
                values[col].update(df[col].dropna().astype(str).unique().tolist())
        offset += limit_per_page
    return {k: sorted(list(v)) for k, v in values.items()}

if __name__ == "__main__":
    filters = sample_values()
    with open("dataHandlers/data/market_filters.json", "w") as f:
        json.dump(filters, f, indent=2)
    print("✅ Saved filter options to market_filters.json")
