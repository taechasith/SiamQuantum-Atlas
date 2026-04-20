"""TI-3: Check Thai institutional domains for RSS/sitemap/robots feasibility."""
from __future__ import annotations
import io, sys, asyncio
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import httpx

DOMAINS = [
    "nstda.or.th",
    "chula.ac.th",
    "sc.chula.ac.th",
    "mahidol.ac.th",
    "sc.mahidol.ac.th",
    "kmutt.ac.th",
    "narit.or.th",
    "tint.or.th",
    "mhesi.go.th",
    "sciencefocus.co",
    "mgronline.com",
]

RSS_PATHS = ["/rss", "/feed", "/rss.xml", "/feed.xml", "/news/rss", "/rss/news"]
SITEMAP_PATHS = ["/sitemap.xml", "/sitemap_index.xml"]


async def check_domain(client: httpx.AsyncClient, domain: str) -> None:
    print(f"\n--- {domain} ---")
    base = f"https://{domain}"

    # robots.txt
    try:
        r = await client.get(f"{base}/robots.txt", timeout=10, follow_redirects=True)
        robots = r.text[:500] if r.status_code == 200 else f"HTTP {r.status_code}"
        disallow_all = "Disallow: /" in robots and "Allow:" not in robots
        print(f"  robots.txt: HTTP {r.status_code} | disallow_all={disallow_all}")
        if "Disallow: /" in robots:
            for line in robots.splitlines():
                if "Disallow" in line or "Allow" in line:
                    print(f"    {line.strip()}")
    except Exception as e:
        print(f"  robots.txt: ERROR {e}")

    # RSS feeds
    rss_found = None
    for path in RSS_PATHS:
        try:
            r = await client.get(f"{base}{path}", timeout=8, follow_redirects=True)
            ct = r.headers.get("content-type","")
            if r.status_code == 200 and ("xml" in ct or "rss" in ct or r.text.strip().startswith("<")):
                rss_found = f"{base}{path}"
                print(f"  RSS: FOUND at {path} (ct={ct[:40]})")
                break
        except Exception:
            pass
    if not rss_found:
        print(f"  RSS: none found")

    # sitemap
    sitemap_found = None
    for path in SITEMAP_PATHS:
        try:
            r = await client.get(f"{base}{path}", timeout=8, follow_redirects=True)
            if r.status_code == 200 and "xml" in r.headers.get("content-type",""):
                sitemap_found = f"{base}{path}"
                print(f"  sitemap: FOUND at {path}")
                break
        except Exception:
            pass
    if not sitemap_found:
        print(f"  sitemap: none found")


async def main() -> None:
    print("TI-3 Thai institutional domain probe")
    async with httpx.AsyncClient(
        headers={"User-Agent": "SiamQuantumAtlas/1.0 (+research)"},
        verify=False,
    ) as client:
        for domain in DOMAINS:
            await check_domain(client, domain)
            await asyncio.sleep(1)
    print("\nDone.")

asyncio.run(main())
