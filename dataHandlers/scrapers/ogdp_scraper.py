# bharatgpt/connectors/ogdp_scraper.py
import requests
from urllib.parse import urlencode
from typing import List, Dict, Any
from ..utils.helpers import utc_now

BASE_URL = "https://www.data.gov.in/backend/dmspublic/v1/resources"

class OGDPScraper:
    def __init__(self, sector=None, ministries=None, limit=100, exact_match=False):
        """
        ministries: List[str] (one or more ministry/department names)
        """
        self.sector = sector
        self.ministries = ministries or []
        self.limit = limit
        self.exact_match = exact_match

    def _make_url(self, offset=0):
        parts = [
            "format=json",
            f"limit={self.limit}",
            f"offset={offset}",
            "sort[published_date]=desc",
            "exact_match=1"
        ]

        if self.sector:
            parts.append(f"filters[sector]={requests.utils.quote(self.sector)}")

        for m in self.ministries:
            parts.append(f"filters[ministry_department][]={requests.utils.quote(m)}")

        return f"{BASE_URL}?{'&'.join(parts)}"


    # --- classify every dataset URL into a known type ---
    def _classify_datafile(self, url: str, fmt: str) -> str:
        url_l = (url or "").lower()
        fmt_l = (fmt or "").lower()

        if not url or "://" not in url_l:
            return "invalid"

        if "api.data.gov.in/resource" in url_l:
            return "json_api"
        if "s3fs-public" in url_l or "ogd20" in url_l:
            return "csv_static"
        if ".asmx" in url_l and "TokenNo=" in url_l:
            return "token_json"
        if "krishi.icar.gov.in" in url_l or "nic.in" in url_l:
            return "portal_page"
        if fmt_l in ("text/json", "application/json"):
            return "json_generic"
        if fmt_l in ("text/csv", "application/csv"):
            return "csv_generic"
        return "unknown"


    def _is_valid_datafile(self, url: str, fmt: str) -> bool:
        if not url or "://" not in url:
            return False

        u = url.lower()
        fmt = (fmt or "").lower()

        # valid structured data
        if "api.data.gov.in/resource" in u:
            return True
        if "s3fs-public" in u or "ogd20" in u:
            return True

        # dynamic but data-bearing (.asmx etc.)
        if ".asmx" in u or "soap" in u or "pmkisan.gov.in" in u:
            return True  # keep for specialized fetcher

        # good textual structured types
        if fmt in ("text/csv", "text/json", "application/json", "application/csv"):
            if "data.gov.in" in u:
                return True

        # known junk domains
        if any(bad in u for bad in ["krishi.icar.gov.in", "publication", "nic.in"]):
            return False

        return False


    def fetch_page(self, offset=0) -> List[Dict[str, Any]]:
        url = self._make_url(offset)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", {}).get("rows", [])
        results = []
        for r in rows:
            url_val = (r.get("datafile_url") or r.get("datafile") or [""])[0]
            fmt = (r.get("file_format") or [""])[0]
            # print(url_val,'\n', fmt)

            if not self._is_valid_datafile(url_val, fmt):
                continue

            src_type = self._classify_datafile(url_val, fmt)

            results.append({
                "id": url_val,
                "title": (r.get("title") or [""])[0],
                "note": " ".join(r.get("note") or []),
                "sector": ", ".join(r.get("sector") or []),
                "ministry": ", ".join(r.get("ministry_department") or []),
                "granularity": ", ".join(r.get("granularity") or []),
                "format": fmt,
                "source_type": src_type,
                "ref_url": ", ".join(r.get("reference_url") or []),
                "scraped_at": utc_now(),
            })
        return results


    def crawl_all(self, max_pages=10) -> List[Dict[str, Any]]:
        all_data = []
        offset = 0
        for _ in range(max_pages):
            page = self.fetch_page(offset)
            if not page:
                break
            all_data.extend(page)
            offset += self.limit
        return all_data

