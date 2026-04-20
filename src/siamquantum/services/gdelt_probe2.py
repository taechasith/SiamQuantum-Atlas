"""TI-1 retry — Q1, Q2, Q5 only (Q3/Q4 already succeeded)."""
from __future__ import annotations
import io, sys, time, httpx
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
PARAMS_BASE = {
    "mode": "ArtList", "maxrecords": "250", "format": "json",
    "startdatetime": "20240101000000", "enddatetime": "20241231235959", "sort": "DateDesc",
}

QUERIES = [
    ("Q1", "quantum sourcecountry:TH", "current baseline"),
    ("Q2", "ควอนตัม sourcecountry:TH", "Thai script only"),
    ("Q5", "(ควอนตัม OR คิวบิต) sourcelang:tha", "Thai vocab (simplified for URL)"),
]

def probe(label: str, query: str, desc: str) -> None:
    print(f"\n{'='*60}")
    print(f"{label}: {desc}")
    try:
        r = httpx.get(BASE, params={**PARAMS_BASE, "query": query}, timeout=30)
        print(f"  HTTP: {r.status_code}")
        if r.status_code != 200:
            print(f"  ERROR: {r.text[:200]}")
            return
        articles = r.json().get("articles") or []
        print(f"  count: {len(articles)}")
        domains: dict[str, int] = {}
        for a in articles:
            try: d = a.get("url","").split("/")[2]
            except: d = "unknown"
            domains[d] = domains.get(d, 0) + 1
        print(f"  top domains: {sorted(domains.items(), key=lambda x:-x[1])[:5]}")
        for a in articles[:3]:
            print(f"    - {(a.get('title','') or '')[:100]}")
    except Exception as exc:
        print(f"  EXCEPTION: {exc}")

print("Waiting 120s before first request...")
time.sleep(120)
for i, (label, query, desc) in enumerate(QUERIES):
    if i > 0:
        print(f"\n[waiting 45s...]")
        time.sleep(45)
    probe(label, query, desc)
print("\nDone.")
