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

        # Add sector only if given (IMD requests should not include this)
        if self.sector:
            parts.append(f"filters[sector]={requests.utils.quote(self.sector)}")

        # Add ministries one by one, literal [] keys unescaped
        for m in self.ministries:
            parts.append(f"filters[ministry_department][]={requests.utils.quote(m)}")

        url = f"{BASE_URL}?{'&'.join(parts)}"
        return url


    def fetch_page(self, offset=0):
        url = self._make_url(offset)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", {}).get("rows", [])

        # Then proceed as before to map rows → our internal dicts
        results = []
        for r in rows:
            url_val = (r.get("datafile_url") or r.get("datafile") or [""])[0]
            results.append({
                "id": url_val,
                "title": (r.get("title") or [""])[0],
                "note": " ".join(r.get("note") or []),
                "sector": ", ".join(r.get("sector") or []),
                "ministry": ", ".join(r.get("ministry_department") or []),
                "granularity": ", ".join(r.get("granularity") or []),
                "format": (r.get("file_format") or [""])[0],
                "ref_url": ", ".join(r.get("reference_url") or []),
                "scraped_at": utc_now(),
            })
        return results


    def fetch_page(self, offset=0) -> List[Dict[str, Any]]:
        url = self._make_url(offset)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", {}).get("rows", [])
        results = []
        for r in rows:
            url_val = (r.get("datafile_url") or r.get("datafile") or [""])[0]
            results.append({
                "id": url_val,
                "title": (r.get("title") or [""])[0],
                "note": " ".join(r.get("note") or []),
                "sector": ", ".join(r.get("sector") or []),
                "ministry": ", ".join(r.get("ministry_department") or []),
                "granularity": ", ".join(r.get("granularity") or []),
                "format": (r.get("file_format") or [""])[0],
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
