"""
Temporary probe script — TI-1 GDELT query comparison for year 2024.
Run: python -m siamquantum.services.gdelt_probe
Enforces 30s delay between each of 5 queries.
"""
from __future__ import annotations

import io
import sys
import time
import httpx

# Force UTF-8 stdout on Windows so Thai chars don't crash
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
PARAMS_BASE = {
    "mode": "ArtList",
    "maxrecords": "250",
    "format": "json",
    "startdatetime": "20240101000000",
    "enddatetime": "20241231235959",
    "sort": "DateDesc",
}

QUERIES = [
    ("Q1", "quantum sourcecountry:TH", "current baseline"),
    ("Q2", "ควอนตัม sourcecountry:TH", "Thai script only"),
    ("Q3", "(quantum OR ควอนตัม) sourcecountry:TH", "union EN+TH"),
    ("Q4", "quantum sourcelang:tha", "language filter"),
    (
        "Q5",
        '(ควอนตัม OR คิวบิต OR "ฟิสิกส์ควอนตัม" OR "คอมพิวเตอร์ควอนตัม") sourcelang:tha',
        "Thai vocab expansion",
    ),
]


def probe_query(label: str, query: str, desc: str) -> None:
    print(f"\n{'='*60}")
    print(f"{label}: {desc}")
    print(f"  query: {query}")
    try:
        r = httpx.get(
            BASE,
            params={**PARAMS_BASE, "query": query},
            timeout=30,
        )
        print(f"  HTTP: {r.status_code}")
        if r.status_code != 200:
            print(f"  ERROR body: {r.text[:200]}")
            return
        data = r.json()
        articles = data.get("articles") or []
        print(f"  count: {len(articles)}")
        domains: dict[str, int] = {}
        for a in articles:
            url = a.get("url", "")
            try:
                domain = url.split("/")[2]
            except IndexError:
                domain = url
            domains[domain] = domains.get(domain, 0) + 1
        top5 = sorted(domains.items(), key=lambda x: -x[1])[:5]
        print(f"  top domains: {top5}")
        print("  sample titles:")
        for a in articles[:3]:
            title = a.get("title", "(no title)")[:100]
            print(f"    - {title}")
    except Exception as exc:
        print(f"  EXCEPTION: {exc}")


def main() -> None:
    print("TI-1 GDELT probe — year=2024, 30s delay between queries")
    for i, (label, query, desc) in enumerate(QUERIES):
        if i > 0:
            print(f"\n[waiting 30s before {label}...]")
            time.sleep(30)
        probe_query(label, query, desc)
    print("\n\nDone.")


if __name__ == "__main__":
    main()
