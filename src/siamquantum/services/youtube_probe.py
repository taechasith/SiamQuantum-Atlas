"""
TI-2 YouTube query probe — year 2024.
Run: python src/siamquantum/services/youtube_probe.py
"""
from __future__ import annotations
import asyncio, io, sys
import httpx
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import os, sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3] / "src"))
from siamquantum.config import settings

KEY = settings.youtube_api_key
SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
CHANNEL_URL = "https://www.googleapis.com/youtube/v3/channels"

QUERIES = [
    ("Q1", {"q": "quantum", "regionCode": "TH", "relevanceLanguage": "th"}, "current baseline"),
    ("Q2", {"q": "ควอนตัม", "regionCode": "TH", "relevanceLanguage": "th"}, "Thai script"),
    ("Q3", {"q": "ฟิสิกส์ควอนตัม OR คอมพิวเตอร์ควอนตัม OR คิวบิต", "regionCode": "TH"}, "Thai vocab"),
    ("Q4", {"q": "quantum computing", "regionCode": "TH", "relevanceLanguage": "th"}, "EN specific term"),
    ("Q5", {"q": "ควอนตัม", "regionCode": "TH"}, "Thai script no lang filter"),
]


async def search(client: httpx.AsyncClient, extra: dict) -> list[dict]:
    params = {
        "part": "snippet",
        "type": "video",
        "maxResults": "50",
        "publishedAfter": "2024-01-01T00:00:00Z",
        "publishedBefore": "2024-12-31T23:59:59Z",
        "key": KEY,
        **extra,
    }
    r = await client.get(SEARCH_URL, params=params, timeout=30)
    if r.status_code != 200:
        print(f"  HTTP {r.status_code}: {r.text[:200]}")
        return []
    return r.json().get("items") or []


async def get_channel_countries(client: httpx.AsyncClient, channel_ids: list[str]) -> dict[str, dict]:
    if not channel_ids:
        return {}
    params = {
        "part": "snippet,brandingSettings",
        "id": ",".join(channel_ids[:50]),
        "key": KEY,
    }
    r = await client.get(CHANNEL_URL, params=params, timeout=30)
    if r.status_code != 200:
        return {}
    result: dict[str, dict] = {}
    for item in r.json().get("items") or []:
        cid = item.get("id","")
        snippet = item.get("snippet", {})
        branding = item.get("brandingSettings", {}).get("channel", {})
        result[cid] = {
            "country": snippet.get("country") or branding.get("country"),
            "defaultLanguage": snippet.get("defaultLanguage"),
            "title": snippet.get("title",""),
        }
    return result


async def main() -> None:
    async with httpx.AsyncClient() as client:
        for i, (label, extra, desc) in enumerate(QUERIES):
            print(f"\n{'='*60}")
            print(f"{label}: {desc}")
            print(f"  params: {extra}")
            items = await search(client, extra)
            print(f"  count: {len(items)}")

            channels: dict[str, int] = {}
            channel_ids: list[str] = []
            for item in items:
                s = item.get("snippet", {})
                ch_id = s.get("channelId","")
                ch_title = s.get("channelTitle","")
                if ch_id:
                    channels[ch_id] = channels.get(ch_id, 0) + 1
                    if ch_id not in channel_ids:
                        channel_ids.append(ch_id)

            top5_ch = sorted(channels.items(), key=lambda x: -x[1])[:5]
            print(f"  top channels (id, count): {top5_ch}")

            for item in items[:3]:
                s = item.get("snippet", {})
                print(f"    - [{s.get('channelTitle','')}] {(s.get('title',''))[:80]}")

            # TI-2.2 — get channel.country for all channels in this query
            if items:
                ch_info = await get_channel_countries(client, channel_ids[:30])
                th_count = sum(1 for v in ch_info.values() if v.get("country") == "TH")
                th_lang = sum(1 for v in ch_info.values() if v.get("defaultLanguage") in ("th", "th-TH"))
                total_ch = len(ch_info)
                print(f"  channels checked: {total_ch}")
                print(f"  channel.country=TH: {th_count}/{total_ch} ({100*th_count//max(total_ch,1)}%)")
                print(f"  channel.defaultLanguage=th: {th_lang}/{total_ch}")

                # Map channel country back to video count
                th_video_count = sum(
                    channels.get(cid, 0)
                    for cid, info in ch_info.items()
                    if info.get("country") == "TH"
                )
                print(f"  videos from TH channels: {th_video_count}/{len(items)}")

                # Sample non-TH channels
                non_th = [(cid, info) for cid, info in ch_info.items() if info.get("country") != "TH"][:3]
                if non_th:
                    print(f"  sample non-TH channels: {[(info['title'], info['country']) for _, info in non_th]}")

            await asyncio.sleep(1)

    print("\n\nDone.")

asyncio.run(main())
