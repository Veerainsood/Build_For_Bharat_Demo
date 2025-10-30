import requests

def resolve_live_download(title: str | None = None, resource_id: str | None = None):
    base = "https://www.data.gov.in/backend/dmspublic/v1/resources"
    params = {"limit": 1}
    if title:
        params["filters[title][contains]"] = title
    if resource_id:
        params["filters[id]"] = resource_id

    r = requests.get(base, params=params, timeout=15)
    r.raise_for_status()
    # breakpoint()
    data = r.json()["records"][0]
    live_url = data.get("datafile_url")
    return live_url, data.get("title"), data.get("id")

if __name__ == "__main__":
    url, title, rid = resolve_live_download(title="RF_SUB_1901-2021")
    print(f"Title: {title}\nResource ID: {rid}\nLive URL: {url}")