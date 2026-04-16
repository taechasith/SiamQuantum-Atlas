#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SiamQuantum Atlas — Pygame Interactive Viewer
Real-time 1000-point Thai quantum media network.

Controls:
  Mouse wheel     zoom in/out
  Right drag      pan
  Left click      select node
  Double-click    open URL in browser
  Tab             switch view  (Timeline ↔ Force)
  Space           play/pause year animation
  R               reset view
  F5              force refresh data from APIs
  Esc             deselect / quit
"""
from __future__ import annotations

import os, sys, json, math, time, threading, webbrowser, hashlib, random
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Set
from dataclasses import dataclass, field
from collections import defaultdict

try:
    import pygame, pygame.freetype
except ImportError:
    sys.exit("Run:  pip install pygame")

# Fix Windows console encoding for Thai characters
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
try:
    import numpy as np
except ImportError:
    sys.exit("Run:  pip install numpy")
try:
    import httpx
except ImportError:
    sys.exit("Run:  pip install httpx")

from dotenv import load_dotenv
load_dotenv()

# ──────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────
YOUTUBE_KEY = os.getenv("SIAMQUANTUM_YOUTUBE_API_KEY", "")
GDELT_BASE  = "https://api.gdeltproject.org/api/v2/doc/doc"
YT_BASE     = "https://www.googleapis.com/youtube/v3"
CACHE_DIR   = Path("data/cache")
CACHE_TTL   = 3600 * 6   # 6 h

W, H          = 1440, 900
SIDEBAR_W     = 240
HEADER_H      = 62
TIMELINE_H    = 84

# ──────────────────────────────────────────────────────────────────
# Theme
# ──────────────────────────────────────────────────────────────────
C_BG     = (  7,  17,  31)
C_BG2    = ( 16,  42,  67)
C_BG3    = ( 24,  58,  90)
C_TEXT   = (243, 247, 251)
C_MUTED  = (159, 179, 200)
C_ACCENT = (240, 180,  41)
C_ACCENT2= ( 46, 134, 171)
C_GREEN  = ( 72, 199, 142)
C_RED    = (255,  80,  80)

PLATFORM_COLOR: Dict[str, Tuple[int,int,int]] = {
    "gdelt_news": ( 46, 134, 171),
    "youtube":    (230,  60,  60),
    "podcast":    ( 72, 199, 142),
    "film_tv":    (200, 130, 210),
}
CLUSTER_COLOR = [
    (240, 180,  41),
    (255,  90,  90),
    ( 46, 134, 171),
    (200, 130, 210),
    ( 72, 199, 142),
    (255, 165,  30),
]
CLUSTER_LABEL = [
    "Science / Computing",
    "Mysticism / Healing",
    "Policy / Security",
    "Entertainment",
    "Education",
    "Business / Industry",
]

# ──────────────────────────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────────────────────────
@dataclass
class Item:
    id: str
    title: str
    platform: str
    published_at: Optional[datetime]
    url: str
    views: float  = 0.0
    likes: float  = 0.0
    comments: float = 0.0
    rank: float   = 50.0
    cluster: int  = 0
    x: float = 0.0
    y: float = 0.0
    vx: float = 0.0
    vy: float = 0.0
    alpha: float = 0.0   # fade-in 0→1
    new_flag: bool = False

    @property
    def engagement(self) -> float:
        if self.views > 0:
            return math.log1p(self.views * 0.01 + self.likes * 0.1 + self.comments * 0.5)
        return math.log1p(max(self.rank, 1))

    @property
    def year(self) -> int:
        return self.published_at.year if self.published_at else 2020

    @property
    def color(self) -> Tuple[int,int,int]:
        return PLATFORM_COLOR.get(self.platform, C_MUTED)

    @property
    def radius(self) -> float:
        return max(3.5, min(18.0, 3.5 + self.engagement * 1.6))


# ──────────────────────────────────────────────────────────────────
# Cluster keyword classifier
# ──────────────────────────────────────────────────────────────────
_KW: List[List[str]] = [
    ["คอมพิวเตอร์","computing","qubit","คิวบิต","ฟิสิกส์","physics","superposition",
     "entanglement","sensing","sensor","เทคโนโลยี","วิจัย","research","algorithm"],
    ["ฮีลลิง","healing","จิตวิญญาณ","spiritual","ดวง","mystical","new age","pseudo",
     "crystal","รักษา","ใจ","ชีวิต","woo","เยียวยา","aura"],
    ["นโยบาย","policy","รัฐบาล","government","มั่นคง","security","ยุทธศาสตร์",
     "national","ชาติ","defense","กฎหมาย","law","เศรษฐกิจ","cyber","cybersecurity"],
    ["ซีรีส์","series","ภาพยนตร์","movie","film","เกม","game","การ์ตูน","anime",
     "sci-fi","ไซไฟ","multiverse","จักรวาล","บันเทิง","นิยาย","fiction"],
    ["อธิบาย","explain","เรียน","learn","สอน","teach","พื้นฐาน","basic","intro",
     "introduction","tutorial","course","คอร์ส","เข้าใจ","ทำไม","what is"],
    ["ธุรกิจ","business","อุตสาหกรรม","industry","ลงทุน","invest","startup",
     "บริษัท","company","market","ตลาด","ผลิต","manufacture","supply"],
]

def classify_cluster(title: str, desc: str = "") -> int:
    text = (title + " " + desc).lower()
    scores = [sum(1 for kw in kws if kw in text) for kws in _KW]
    best = max(range(len(scores)), key=lambda i: scores[i])
    return best if scores[best] > 0 else 0


# ──────────────────────────────────────────────────────────────────
# Disk cache
# ──────────────────────────────────────────────────────────────────
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _cpath(key: str) -> Path:
    return CACHE_DIR / f"{hashlib.md5(key.encode()).hexdigest()}.json"

def cache_load(key: str) -> Optional[list]:
    p = _cpath(key)
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text("utf-8"))
        if time.time() - d["ts"] < CACHE_TTL:
            return d["records"]
    except Exception:
        pass
    return None

def cache_save(key: str, records: list) -> None:
    _cpath(key).write_text(
        json.dumps({"ts": time.time(), "records": records}, ensure_ascii=False),
        encoding="utf-8",
    )


# ──────────────────────────────────────────────────────────────────
# GDELT fetcher
# ──────────────────────────────────────────────────────────────────
_GDELT_QUERIES = [
    'ควอนตัม sourcelang:tha',
    'quantum sourcelang:tha',
    'คอมพิวเตอร์ควอนตัม',
    'ฟิสิกส์ควอนตัม',
]

def _parse_gdelt_date(s: str) -> Optional[str]:
    if not s:
        return None
    # GDELT formats: "20240510T090000Z" or "20240510"
    clean = s.strip()
    for fmt, length in [("%Y%m%dT%H%M%SZ", 16), ("%Y%m%d", 8)]:
        try:
            return datetime.strptime(clean[:length], fmt).isoformat()
        except Exception:
            pass
    return None

def _gdelt_year(q: str, year: int, client: httpx.Client, retry: int = 2) -> list:
    key = f"g_{q}_{year}"
    cached = cache_load(key)
    if cached is not None:
        return cached
    for attempt in range(retry + 1):
        try:
            r = client.get(GDELT_BASE, params={
                "query": q, "mode": "ArtList",
                "maxrecords": "250", "format": "json",
                "startdatetime": f"{year}0101000000",
                "enddatetime":   f"{year}1231235959",
            }, timeout=25)
            if r.status_code == 429:
                wait = 3 * (attempt + 1)
                print(f"  GDELT rate-limited, waiting {wait}s…")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                return []
            data = r.json()
            recs = [{
                "platform": "gdelt_news",
                "title": a.get("title",""),
                "url": a.get("url",""),
                "published_at": _parse_gdelt_date(a.get("seendate","")),
                "domain": a.get("domain",""),
                "description": "",
                "rank": float(a.get("socialshares") or 50),
            } for a in data.get("articles", [])]
            cache_save(key, recs)
            return recs
        except Exception as e:
            print(f"  GDELT {q} {year}: {e}")
            if attempt < retry:
                time.sleep(2)
    return []

def fetch_gdelt_all(client: httpx.Client, on_progress=None) -> list:
    out, seen = [], set()
    for q in _GDELT_QUERIES[:2]:
        for year in range(2015, 2026):
            recs = _gdelt_year(q, year, client)
            for r in recs:
                u = r["url"]
                if u and u not in seen:
                    seen.add(u); out.append(r)
            if on_progress: on_progress(len(out))
            time.sleep(0.08)
        if len(out) >= 700:
            break
    return out


# ──────────────────────────────────────────────────────────────────
# YouTube fetcher
# ──────────────────────────────────────────────────────────────────
_YT_QUERIES = [
    "ควอนตัม",
    "ฟิสิกส์ควอนตัม",
    "คอมพิวเตอร์ควอนตัม",
    "ควอนตัมฮีลลิง",
    "quantum computing ภาษาไทย",
]

def _yt_query(q: str, client: httpx.Client, pages: int = 4) -> list:
    if not YOUTUBE_KEY:
        return []
    key = f"yt_{q}_{pages}"
    cached = cache_load(key)
    if cached is not None:
        return cached

    vids_raw, vid_ids = [], []
    page_token = None
    for _ in range(pages):
        params = {
            "part": "snippet", "q": q, "type": "video",
            "maxResults": 50, "key": YOUTUBE_KEY,
            "relevanceLanguage": "th",
            "publishedAfter": "2015-01-01T00:00:00Z",
        }
        if page_token:
            params["pageToken"] = page_token
        try:
            r = client.get(f"{YT_BASE}/search", params=params, timeout=20)
            data = r.json()
            if "error" in data:
                print(f"  YT error: {data['error']['message']}")
                break
            for item in data.get("items", []):
                vid = item["id"].get("videoId")
                if vid:
                    vid_ids.append(vid)
                    vids_raw.append(item)
            page_token = data.get("nextPageToken")
            if not page_token:
                break
            time.sleep(0.12)
        except Exception as e:
            print(f"  YT search {q}: {e}"); break

    # stats
    stats: Dict[str, dict] = {}
    for i in range(0, len(vid_ids), 50):
        batch = vid_ids[i:i+50]
        try:
            r = client.get(f"{YT_BASE}/videos", params={
                "part": "statistics", "id": ",".join(batch), "key": YOUTUBE_KEY,
            }, timeout=20)
            for item in r.json().get("items", []):
                stats[item["id"]] = item.get("statistics", {})
            time.sleep(0.1)
        except Exception:
            pass

    recs, seen = [], set()
    for item in vids_raw:
        vid = item["id"].get("videoId")
        if not vid or vid in seen:
            continue
        seen.add(vid)
        sn = item["snippet"]
        s  = stats.get(vid, {})
        recs.append({
            "platform": "youtube",
            "title": sn.get("title",""),
            "url": f"https://www.youtube.com/watch?v={vid}",
            "published_at": sn.get("publishedAt","")[:19],
            "domain": "youtube.com",
            "description": sn.get("description","")[:300],
            "views":    float(s.get("viewCount",   0) or 0),
            "likes":    float(s.get("likeCount",    0) or 0),
            "comments": float(s.get("commentCount", 0) or 0),
        })
    cache_save(key, recs)
    return recs

def fetch_youtube_all(client: httpx.Client, on_progress=None, offset: int = 0) -> list:
    out, seen = [], set()
    needed = max(300, 1000 - offset)
    pages  = min(5, math.ceil(needed / (50 * len(_YT_QUERIES))) + 1)
    for q in _YT_QUERIES:
        recs = _yt_query(q, client, pages=pages)
        for r in recs:
            u = r["url"]
            if u and u not in seen:
                seen.add(u); out.append(r)
        if on_progress: on_progress(offset + len(out))
    return out


# ──────────────────────────────────────────────────────────────────
# Master fetch + layout
# ──────────────────────────────────────────────────────────────────
def _to_datetime(s: str) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except Exception:
            pass
    return None

def _assign_timeline_positions(items: List[Item]) -> None:
    rng = random.Random(42)
    for item in items:
        x = (item.year - 2015) / 10.0
        y = (item.cluster + 0.5) / len(CLUSTER_LABEL)
        item.x = max(0.02, min(0.98, x + rng.gauss(0, 0.018)))
        item.y = max(0.02, min(0.98, y + rng.gauss(0, 0.022)))

def fetch_all_data(on_progress=None) -> List[Item]:
    items: List[Item] = []
    seen_urls: Set[str] = set()
    uid = 0

    def prog(n):
        if on_progress: on_progress(n)

    print("Fetching GDELT data…")
    with httpx.Client(follow_redirects=True) as client:
        gdelt_recs = fetch_gdelt_all(client, on_progress=prog)
        print(f"  GDELT: {len(gdelt_recs)} articles")
        yt_recs    = fetch_youtube_all(client, on_progress=prog, offset=len(gdelt_recs))
        print(f"  YouTube: {len(yt_recs)} videos")

    all_recs = gdelt_recs + yt_recs
    for r in all_recs:
        url = r.get("url","")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        uid += 1
        pub = _to_datetime(r.get("published_at","") or "")
        item = Item(
            id=f"item:{uid}",
            title=r.get("title","(no title)"),
            platform=r.get("platform","gdelt_news"),
            published_at=pub,
            url=url,
            views=float(r.get("views",0) or 0),
            likes=float(r.get("likes",0) or 0),
            comments=float(r.get("comments",0) or 0),
            rank=float(r.get("rank",50) or 50),
            cluster=classify_cluster(r.get("title",""), r.get("description","")),
        )
        items.append(item)

    _assign_timeline_positions(items)
    print(f"  Total unique items: {len(items)}")
    return items


# ──────────────────────────────────────────────────────────────────
# Force-directed layout (background thread)
# ──────────────────────────────────────────────────────────────────
class ForceLayout:
    def __init__(self):
        self.lock = threading.Lock()
        self.positions: Optional[np.ndarray] = None
        self.running = False
        self._thread: Optional[threading.Thread] = None

    def start(self, items: List[Item]) -> None:
        self.running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)
        pos = np.array([[i.x, i.y] for i in items], dtype=np.float32)
        self.running = True
        self._thread = threading.Thread(target=self._run, args=(pos, items), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.running = False

    def _run(self, pos: np.ndarray, items: List[Item]) -> None:
        n = len(pos)
        vel = np.zeros_like(pos)
        dt, cool = 0.015, 0.98

        # Cluster centres in a ring
        k = len(CLUSTER_LABEL)
        centres = np.array([
            [0.5 + 0.35 * math.cos(2*math.pi*i/k),
             0.5 + 0.35 * math.sin(2*math.pi*i/k)]
            for i in range(k)
        ], dtype=np.float32)
        cluster_ids = np.array([i.cluster for i in items], dtype=np.int32)

        step = 0
        while self.running and step < 600:
            step += 1
            # Repulsion (vectorised, O(n²) but fast in numpy for n≤1000)
            diff  = pos[:, np.newaxis, :] - pos[np.newaxis, :, :]   # (n,n,2)
            dist2 = (diff ** 2).sum(axis=2) + 1e-4                   # (n,n)
            np.fill_diagonal(dist2, np.inf)
            rep   = (0.0008 / dist2)[:, :, np.newaxis] * diff
            f = rep.sum(axis=1)

            # Attraction to cluster centre
            for ci in range(k):
                mask = (cluster_ids == ci)
                if not mask.any(): continue
                delta = centres[ci] - pos[mask]
                f[mask] += delta * 0.08

            # Centre gravity
            f += (0.5 - pos) * 0.03

            vel = (vel + f * dt) * cool
            pos = np.clip(pos + vel * dt, 0.02, 0.98)

            with self.lock:
                self.positions = pos.copy()

            time.sleep(0.005)

    def apply(self, items: List[Item]) -> None:
        with self.lock:
            if self.positions is not None and len(self.positions) == len(items):
                for i, item in enumerate(items):
                    item.x, item.y = float(self.positions[i, 0]), float(self.positions[i, 1])


# ──────────────────────────────────────────────────────────────────
# DataManager
# ──────────────────────────────────────────────────────────────────
class DataManager:
    def __init__(self):
        self.items: List[Item] = []
        self.loading = False
        self.progress = 0
        self.status = "Initializing…"
        self.last_update: Optional[datetime] = None
        self.lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None

    def start_fetch(self, force: bool = False) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, args=(force,), daemon=True)
        self._thread.start()

    def _run(self, force: bool) -> None:
        self.loading = True
        self.status = "Fetching real data…"
        try:
            def prog(n: int):
                self.progress = n
                self.status = f"Loaded {n} items…"

            items = fetch_all_data(on_progress=prog)
            with self.lock:
                self.items = items
                self.last_update = datetime.now()
            self.status = f"Ready — {len(items)} items"
        except Exception as e:
            self.status = f"Error: {e}"
            print(f"DataManager error: {e}")
        finally:
            self.loading = False

    @property
    def count(self) -> int:
        return len(self.items)


# ──────────────────────────────────────────────────────────────────
# Viewport
# ──────────────────────────────────────────────────────────────────
CX0 = SIDEBAR_W
CY0 = HEADER_H
CW  = W - SIDEBAR_W
CH  = H - HEADER_H - TIMELINE_H

class Viewport:
    def __init__(self):
        self.zoom = 0.85
        self.pan_x = 0.0
        self.pan_y = 0.0

    def reset(self) -> None:
        self.zoom = 0.85
        self.pan_x = 0.0
        self.pan_y = 0.0

    def w2s(self, wx: float, wy: float) -> Tuple[float, float]:
        sx = CX0 + (wx - self.pan_x) * self.zoom * CW + (1 - self.zoom) * CW * 0.5
        sy = CY0 + (wy - self.pan_y) * self.zoom * CH + (1 - self.zoom) * CH * 0.5
        return sx, sy

    def s2w(self, sx: float, sy: float) -> Tuple[float, float]:
        wx = (sx - CX0 - (1 - self.zoom) * CW * 0.5) / (self.zoom * CW) + self.pan_x
        wy = (sy - CY0 - (1 - self.zoom) * CH * 0.5) / (self.zoom * CH) + self.pan_y
        return wx, wy

    def scroll(self, dy: int, mouse_sx: float, mouse_sy: float) -> None:
        old_wx, old_wy = self.s2w(mouse_sx, mouse_sy)
        factor = 1.12 if dy > 0 else 1/1.12
        self.zoom = max(0.25, min(8.0, self.zoom * factor))
        new_wx, new_wy = self.s2w(mouse_sx, mouse_sy)
        self.pan_x += old_wx - new_wx
        self.pan_y += old_wy - new_wy


# ──────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────
def _mix(a: Tuple[int,...], b: Tuple[int,...], t: float) -> Tuple[int,...]:
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(len(a)))

def _draw_text(surf, font, text: str, pos: Tuple[int,int],
               color=C_TEXT, bg=None, anchor="topleft") -> pygame.Rect:
    text_surf, rect = font.render(text, color, bg)
    r = text_surf.get_rect(**{anchor: pos})
    surf.blit(text_surf, r)
    return r

def _draw_rounded_rect(surf, rect, color, radius=8, alpha=255) -> None:
    s = pygame.Surface((rect.width, rect.height), pygame.SRCALPHA)
    pygame.draw.rect(s, (*color, alpha), s.get_rect(), border_radius=radius)
    surf.blit(s, rect.topleft)

def _glow_circle(surf, cx, cy, r, color, intensity=0.35) -> None:
    for dr in range(int(r*1.8), int(r)-1, -2):
        a = int(255 * intensity * (1 - (dr - r) / (r * 0.8 + 0.1)))
        if a <= 0: continue
        s = pygame.Surface((dr*2, dr*2), pygame.SRCALPHA)
        pygame.draw.circle(s, (*color, a), (dr, dr), dr)
        surf.blit(s, (int(cx-dr), int(cy-dr)), special_flags=pygame.BLEND_RGBA_ADD)


# ──────────────────────────────────────────────────────────────────
# Main App
# ──────────────────────────────────────────────────────────────────
VIEW_TIMELINE = "timeline"
VIEW_FORCE    = "force"

class App:
    def __init__(self):
        pygame.init()
        pygame.freetype.init()
        self.screen = pygame.display.set_mode((W, H), pygame.RESIZABLE)
        pygame.display.set_caption("SiamQuantum Atlas — Thai Quantum Media Network")

        base = Path(__file__).parent
        font_candidates = [
            base / "viewer" / "NotoSansThai.ttf",   # put Thai font here if you have one
        ]
        font_path = next((p for p in font_candidates if p.exists()), None)
        self.font_lg  = pygame.freetype.SysFont("segoeui", 17)
        self.font_md  = pygame.freetype.SysFont("segoeui", 13)
        self.font_sm  = pygame.freetype.SysFont("segoeui", 11)
        self.font_xl  = pygame.freetype.SysFont("segoeui", 22, bold=True)
        if font_path:
            self.font_lg = pygame.freetype.Font(str(font_path), 17)
            self.font_md = pygame.freetype.Font(str(font_path), 13)
            self.font_sm = pygame.freetype.Font(str(font_path), 11)

        self.clock   = pygame.time.Clock()
        self.vp      = Viewport()
        self.dm      = DataManager()
        self.force   = ForceLayout()

        self.view           = VIEW_TIMELINE
        self.selected: Optional[Item] = None
        self.hovered: Optional[Item]  = None
        self.dragging_pan   = False
        self.drag_start     = (0, 0)
        self.drag_pan_start = (0.0, 0.0)

        # Timeline scrubber
        self.year_min = 2015
        self.year_max = 2025
        self.year_cursor = 2025  # show up to this year
        self.playing = False
        self.play_speed = 0.3    # years per second
        self.tl_dragging = False

        # Filters
        self.platform_filter: Set[str] = set(PLATFORM_COLOR.keys())
        self.cluster_filter:  Set[int] = set(range(len(CLUSTER_LABEL)))

        # Stats
        self._fps_samples: List[float] = []

        # Start data fetch
        self.dm.start_fetch()

        self._force_active = False
        self._last_refresh = time.time()

    # ─── main loop ───────────────────────────────────────────────
    def run(self) -> None:
        while True:
            dt = self.clock.tick(60) / 1000.0
            if not self._handle_events():
                break
            self._update(dt)
            self._render()

    # ─── events ──────────────────────────────────────────────────
    def _handle_events(self) -> bool:
        for ev in pygame.event.get():
            if ev.type == pygame.QUIT:
                return False
            elif ev.type == pygame.KEYDOWN:
                if ev.key == pygame.K_ESCAPE:
                    if self.selected:
                        self.selected = None
                    else:
                        return False
                elif ev.key == pygame.K_r:
                    self.vp.reset()
                elif ev.key == pygame.K_TAB:
                    self._toggle_view()
                elif ev.key == pygame.K_SPACE:
                    self.playing = not self.playing
                    if self.playing and self.year_cursor >= self.year_max:
                        self.year_cursor = float(self.year_min)
                elif ev.key == pygame.K_F5:
                    self._force_refresh()
            elif ev.type == pygame.MOUSEBUTTONDOWN:
                if ev.button == 3:  # right drag = pan
                    self.dragging_pan = True
                    self.drag_start = ev.pos
                    self.drag_pan_start = (self.vp.pan_x, self.vp.pan_y)
                elif ev.button == 1:
                    self._handle_click(ev.pos, ev.type)
            elif ev.type == pygame.MOUSEBUTTONUP:
                if ev.button == 3:
                    self.dragging_pan = False
                elif ev.button == 1:
                    self.tl_dragging = False
            elif ev.type == pygame.MOUSEMOTION:
                self._handle_motion(ev.pos)
            elif ev.type == pygame.MOUSEWHEEL:
                mx, my = pygame.mouse.get_pos()
                if CX0 <= mx <= W and CY0 <= my <= CY0 + CH:
                    self.vp.scroll(ev.y, mx, my)
            elif ev.type == pygame.VIDEORESIZE:
                pass
        return True

    def _toggle_view(self) -> None:
        if self.view == VIEW_TIMELINE:
            self.view = VIEW_FORCE
            items = self.dm.items
            if items and not self._force_active:
                self._force_active = True
                self.force.start(items)
        else:
            self.view = VIEW_TIMELINE
            self.force.stop()
            _assign_timeline_positions(self.dm.items)

    def _force_refresh(self) -> None:
        # Clear cache and restart fetch
        for p in CACHE_DIR.glob("*.json"):
            p.unlink(missing_ok=True)
        self.dm.start_fetch(force=True)
        self.force.stop()
        self._force_active = False

    def _handle_click(self, pos: Tuple[int,int], ev_type: int) -> None:
        mx, my = pos
        # Timeline bar click
        tl_y = H - TIMELINE_H
        if tl_y <= my <= H:
            self.tl_dragging = True
            self._update_tl(mx)
            return
        # Sidebar clicks (platform/cluster filters)
        if mx < SIDEBAR_W:
            self._sidebar_click(mx, my)
            return
        # Canvas click
        hit = self._hit_test(mx, my)
        if hit:
            self.selected = hit
        else:
            self.selected = None

    def _sidebar_click(self, mx: int, my: int) -> None:
        # Platform toggle rows start at y=200
        y = 200
        for p in list(PLATFORM_COLOR.keys()):
            if y <= my <= y + 24:
                if p in self.platform_filter:
                    self.platform_filter.discard(p)
                else:
                    self.platform_filter.add(p)
                return
            y += 28
        # Cluster toggle rows
        y += 24
        for ci in range(len(CLUSTER_LABEL)):
            if y <= my <= y + 22:
                if ci in self.cluster_filter:
                    self.cluster_filter.discard(ci)
                else:
                    self.cluster_filter.add(ci)
                return
            y += 26

    def _handle_motion(self, pos: Tuple[int,int]) -> None:
        mx, my = pos
        if self.dragging_pan:
            dx = (mx - self.drag_start[0]) / (self.vp.zoom * CW)
            dy = (my - self.drag_start[1]) / (self.vp.zoom * CH)
            self.vp.pan_x = self.drag_pan_start[0] - dx
            self.vp.pan_y = self.drag_pan_start[1] - dy
        if self.tl_dragging:
            self._update_tl(mx)
        self.hovered = self._hit_test(mx, my)

    def _update_tl(self, mx: int) -> None:
        rel = (mx - CX0) / max(CW, 1)
        yr = self.year_min + rel * (self.year_max - self.year_min)
        self.year_cursor = max(float(self.year_min), min(float(self.year_max), yr))

    def _hit_test(self, mx: int, my: int) -> Optional[Item]:
        if not (CX0 <= mx <= W and CY0 <= my <= CY0 + CH):
            return None
        best: Optional[Item] = None
        best_d = float("inf")
        for item in self._visible_items():
            sx, sy = self.vp.w2s(item.x, item.y)
            r = item.radius * self.vp.zoom + 4
            d = math.hypot(mx - sx, my - sy)
            if d <= r and d < best_d:
                best_d = d; best = item
        return best

    # ─── update ──────────────────────────────────────────────────
    def _update(self, dt: float) -> None:
        # Fade in new items
        for item in self.dm.items:
            if item.alpha < 1.0:
                item.alpha = min(1.0, item.alpha + dt * 2)

        # Apply force layout positions
        if self.view == VIEW_FORCE and self._force_active:
            self.force.apply(self.dm.items)

        # Timeline animation
        if self.playing:
            self.year_cursor = min(self.year_max, self.year_cursor + self.play_speed * dt)
            if self.year_cursor >= self.year_max:
                self.playing = False

        # FPS
        self._fps_samples.append(self.clock.get_fps())
        if len(self._fps_samples) > 60:
            self._fps_samples.pop(0)

        # Auto-refresh every 30 min
        if time.time() - self._last_refresh > 1800 and not self.dm.loading:
            self._last_refresh = time.time()
            self.dm.start_fetch()

    # ─── render ──────────────────────────────────────────────────
    def _render(self) -> None:
        self.screen.fill(C_BG)
        self._draw_canvas_bg()
        self._draw_nodes()
        self._draw_cluster_labels()
        self._draw_header()
        self._draw_sidebar()
        self._draw_timeline_bar()
        if self.hovered and self.hovered is not self.selected:
            self._draw_tooltip(self.hovered)
        if self.selected:
            self._draw_detail_panel(self.selected)
        self._draw_loading_overlay()
        pygame.display.flip()

    def _draw_canvas_bg(self) -> None:
        canvas = pygame.Rect(CX0, CY0, CW, CH)
        pygame.draw.rect(self.screen, C_BG2, canvas)
        # Grid lines for timeline view
        if self.view == VIEW_TIMELINE:
            for yr in range(2015, 2026):
                sx, _ = self.vp.w2s((yr - 2015) / 10.0, 0)
                if CX0 <= sx <= W:
                    pygame.draw.line(self.screen, C_BG3, (int(sx), CY0), (int(sx), CY0+CH), 1)
        # Border
        pygame.draw.rect(self.screen, C_BG3, canvas, 1)

    def _visible_items(self) -> List[Item]:
        items = self.dm.items
        yr = int(self.year_cursor)
        return [
            i for i in items
            if i.platform in self.platform_filter
            and i.cluster in self.cluster_filter
            and i.year <= yr
        ]

    def _draw_nodes(self) -> None:
        surf = self.screen
        visible = self._visible_items()

        # Draw cluster hulls (simple circles per cluster centre)
        if self.view == VIEW_FORCE:
            by_cluster: Dict[int, List[Item]] = defaultdict(list)
            for item in visible:
                by_cluster[item.cluster].append(item)
            for ci, cluster_items in by_cluster.items():
                if len(cluster_items) < 2:
                    continue
                xs = [self.vp.w2s(i.x, i.y)[0] for i in cluster_items]
                ys = [self.vp.w2s(i.x, i.y)[1] for i in cluster_items]
                cx, cy = sum(xs)/len(xs), sum(ys)/len(ys)
                r = max(30, int(max(max(abs(x-cx) for x in xs), max(abs(y-cy) for y in ys)) * 1.2))
                s = pygame.Surface((r*2, r*2), pygame.SRCALPHA)
                col = CLUSTER_COLOR[ci % len(CLUSTER_COLOR)]
                pygame.draw.circle(s, (*col, 18), (r, r), r)
                surf.blit(s, (int(cx-r), int(cy-r)))

        for item in sorted(visible, key=lambda i: i.engagement):
            sx, sy = self.vp.w2s(item.x, item.y)
            if not (CX0 - 20 <= sx <= W + 20 and CY0 - 20 <= sy <= CY0 + CH + 20):
                continue
            r = max(2, int(item.radius * self.vp.zoom))
            col = item.color
            alpha = int(255 * item.alpha)

            is_sel = item is self.selected
            is_hov = item is self.hovered

            if is_sel or is_hov:
                _glow_circle(surf, sx, sy, r + 3, col, 0.5 if is_sel else 0.3)

            # Draw node
            if r >= 2:
                s = pygame.Surface((r*2+2, r*2+2), pygame.SRCALPHA)
                pygame.draw.circle(s, (*col, alpha), (r+1, r+1), r)
                if is_sel:
                    pygame.draw.circle(s, (*C_ACCENT, 255), (r+1, r+1), r, 2)
                surf.blit(s, (int(sx-r-1), int(sy-r-1)))

    def _draw_cluster_labels(self) -> None:
        if self.view == VIEW_TIMELINE:
            for ci, label in enumerate(CLUSTER_LABEL):
                wy = (ci + 0.5) / len(CLUSTER_LABEL)
                _, sy = self.vp.w2s(0, wy)
                if CY0 <= sy <= CY0 + CH:
                    col = CLUSTER_COLOR[ci % len(CLUSTER_COLOR)]
                    _draw_text(self.screen, self.font_sm, label,
                               (CX0 + 6, int(sy)), color=(*col, 160))
        elif self.view == VIEW_FORCE:
            by_cluster: Dict[int, List[Item]] = defaultdict(list)
            for item in self._visible_items():
                by_cluster[item.cluster].append(item)
            for ci, cluster_items in by_cluster.items():
                if not cluster_items: continue
                sx = sum(self.vp.w2s(i.x, i.y)[0] for i in cluster_items) / len(cluster_items)
                sy = sum(self.vp.w2s(i.x, i.y)[1] for i in cluster_items) / len(cluster_items)
                label = CLUSTER_LABEL[ci % len(CLUSTER_LABEL)]
                col = CLUSTER_COLOR[ci % len(CLUSTER_COLOR)]
                _draw_rounded_rect(self.screen,
                    pygame.Rect(int(sx)-60, int(sy)-10, 120, 20),
                    C_BG, radius=4, alpha=180)
                _draw_text(self.screen, self.font_sm, label,
                           (int(sx), int(sy)), color=col, anchor="center")

    def _draw_header(self) -> None:
        # Background
        _draw_rounded_rect(self.screen, pygame.Rect(0, 0, W, HEADER_H), C_BG2, radius=0, alpha=230)
        pygame.draw.line(self.screen, C_BG3, (0, HEADER_H), (W, HEADER_H), 1)

        # Title
        _draw_text(self.screen, self.font_xl, "SiamQuantum Atlas",
                   (18, 16), color=C_TEXT)
        _draw_text(self.screen, self.font_sm, "Thai Quantum Media Network  ·  10 Years  ·  Real Data",
                   (18, 40), color=C_MUTED)

        # Live indicator
        pulse = 0.5 + 0.5 * math.sin(time.time() * 3)
        live_col = _mix(C_MUTED, C_GREEN, pulse) if not self.dm.loading else _mix(C_MUTED, C_ACCENT, pulse)
        pygame.draw.circle(self.screen, live_col, (320, 24), 5)
        status_text = self.dm.status if self.dm.loading else (
            f"{'⏸' if not self.playing else '▶'} {int(self.year_cursor)} · {self.dm.count} items"
        )
        _draw_text(self.screen, self.font_sm, status_text, (330, 18), color=C_MUTED)

        # View toggle pill
        pill_x = W - 260
        _draw_rounded_rect(self.screen, pygame.Rect(pill_x, 14, 200, 34), C_BG3, radius=17)
        for i, (lbl, v) in enumerate([("Timeline", VIEW_TIMELINE), ("Force Graph", VIEW_FORCE)]):
            bx = pill_x + i * 100 + 4
            active = self.view == v
            if active:
                _draw_rounded_rect(self.screen, pygame.Rect(bx, 16, 96, 30), C_ACCENT, radius=15)
            col = C_BG if active else C_MUTED
            _draw_text(self.screen, self.font_sm, lbl, (bx+48, 31), color=col, anchor="center")

        # FPS
        fps = sum(self._fps_samples)/max(1,len(self._fps_samples))
        _draw_text(self.screen, self.font_sm, f"{fps:.0f} fps",
                   (W - 50, HEADER_H//2), color=C_BG3, anchor="center")

    def _draw_sidebar(self) -> None:
        pygame.draw.rect(self.screen, C_BG2, pygame.Rect(0, HEADER_H, SIDEBAR_W, H - HEADER_H))
        pygame.draw.line(self.screen, C_BG3, (SIDEBAR_W, HEADER_H), (SIDEBAR_W, H), 1)

        y = HEADER_H + 14
        _draw_text(self.screen, self.font_md, "PLATFORMS", (12, y), color=C_ACCENT)
        y += 22

        # Platform counts
        by_plat: Dict[str, int] = defaultdict(int)
        for i in self.dm.items:
            by_plat[i.platform] += 1

        for plat, col in PLATFORM_COLOR.items():
            active = plat in self.platform_filter
            bg = (*C_BG3, 200) if active else (*C_BG, 200)
            _draw_rounded_rect(self.screen, pygame.Rect(8, y, SIDEBAR_W-16, 24), C_BG3 if active else C_BG, radius=5)
            pygame.draw.circle(self.screen, col if active else C_BG3, (22, y+12), 6)
            name = {"gdelt_news":"News (GDELT)","youtube":"YouTube","podcast":"Podcast","film_tv":"Film/TV"}.get(plat,plat)
            _draw_text(self.screen, self.font_sm, name, (32, y+12), color=C_TEXT if active else C_MUTED, anchor="midleft")
            cnt = by_plat.get(plat, 0)
            _draw_text(self.screen, self.font_sm, str(cnt), (SIDEBAR_W-12, y+12), color=C_ACCENT if active else C_BG3, anchor="midright")
            y += 28

        y += 12
        pygame.draw.line(self.screen, C_BG3, (8, y), (SIDEBAR_W-8, y), 1)
        y += 12
        _draw_text(self.screen, self.font_md, "CLUSTERS", (12, y), color=C_ACCENT)
        y += 22

        by_cl: Dict[int, int] = defaultdict(int)
        for i in self.dm.items:
            by_cl[i.cluster] += 1

        for ci, label in enumerate(CLUSTER_LABEL):
            active = ci in self.cluster_filter
            col = CLUSTER_COLOR[ci % len(CLUSTER_COLOR)]
            _draw_rounded_rect(self.screen, pygame.Rect(8, y, SIDEBAR_W-16, 22), C_BG3 if active else C_BG, radius=4)
            pygame.draw.rect(self.screen, col if active else C_BG3, pygame.Rect(10, y+5, 8, 12), border_radius=2)
            _draw_text(self.screen, self.font_sm, label, (24, y+11), color=C_TEXT if active else C_MUTED, anchor="midleft")
            _draw_text(self.screen, self.font_sm, str(by_cl.get(ci,0)), (SIDEBAR_W-12, y+11), color=C_ACCENT if active else C_BG3, anchor="midright")
            y += 26

        y += 16
        pygame.draw.line(self.screen, C_BG3, (8, y), (SIDEBAR_W-8, y), 1)
        y += 12

        # Stats
        _draw_text(self.screen, self.font_md, "STATS", (12, y), color=C_ACCENT)
        y += 22
        vis = len(self._visible_items())
        for label, val in [
            ("Total items", self.dm.count),
            ("Visible",     vis),
            ("Platforms",   len(self.platform_filter)),
            ("Year range",  f"2015–{int(self.year_cursor)}"),
        ]:
            _draw_text(self.screen, self.font_sm, label, (12, y), color=C_MUTED)
            _draw_text(self.screen, self.font_sm, str(val), (SIDEBAR_W-12, y), color=C_TEXT, anchor="topright")
            y += 18

        # Controls hint
        y = H - TIMELINE_H - 130
        _draw_text(self.screen, self.font_sm, "CONTROLS", (12, y), color=C_ACCENT); y += 18
        for hint in ["Scroll: zoom", "Right drag: pan", "Click: select", "Tab: toggle view",
                     "Space: play time", "F5: refresh data"]:
            _draw_text(self.screen, self.font_sm, hint, (12, y), color=C_MUTED); y += 16

    def _draw_timeline_bar(self) -> None:
        bar_y = H - TIMELINE_H
        pygame.draw.rect(self.screen, C_BG2, pygame.Rect(0, bar_y, W, TIMELINE_H))
        pygame.draw.line(self.screen, C_BG3, (0, bar_y), (W, bar_y), 1)

        # Year labels
        track_x0 = CX0 + 30
        track_x1 = W - 30
        track_w  = track_x1 - track_x0
        track_y  = bar_y + 36

        pygame.draw.rect(self.screen, C_BG3, pygame.Rect(track_x0, track_y - 2, track_w, 4), border_radius=2)

        for yr in range(2015, 2026):
            tx = track_x0 + int((yr - 2015) / 10.0 * track_w)
            pygame.draw.line(self.screen, C_BG3, (tx, track_y - 6), (tx, track_y + 6), 1)
            _draw_text(self.screen, self.font_sm, str(yr), (tx, track_y + 10), color=C_MUTED, anchor="midtop")

        # Filled progress
        cx = track_x0 + int((self.year_cursor - 2015) / 10.0 * track_w)
        pygame.draw.rect(self.screen, (*C_ACCENT, 160),
                         pygame.Rect(track_x0, track_y-2, cx - track_x0, 4), border_radius=2)
        pygame.draw.circle(self.screen, C_ACCENT, (cx, track_y), 8)
        _draw_text(self.screen, self.font_md, f"{int(self.year_cursor)}",
                   (cx, track_y - 18), color=C_ACCENT, anchor="midbottom")

        # Play button
        play_x = track_x0 - 22
        col = C_GREEN if self.playing else C_MUTED
        if self.playing:
            pygame.draw.rect(self.screen, col, pygame.Rect(play_x - 5, track_y - 7, 5, 14), border_radius=1)
            pygame.draw.rect(self.screen, col, pygame.Rect(play_x + 2, track_y - 7, 5, 14), border_radius=1)
        else:
            pts = [(play_x-4, track_y-8), (play_x+8, track_y), (play_x-4, track_y+8)]
            pygame.draw.polygon(self.screen, col, pts)

        # Density chart above scrubber
        by_year: Dict[int, int] = defaultdict(int)
        for i in self.dm.items:
            if i.platform in self.platform_filter and i.cluster in self.cluster_filter:
                by_year[i.year] += 1
        if by_year:
            max_cnt = max(by_year.values()) or 1
            bar_top = bar_y + 6
            bar_h   = 18
            for yr in range(2015, 2026):
                cnt = by_year.get(yr, 0)
                bx = track_x0 + int((yr - 2015) / 10.0 * track_w)
                bw = max(1, int(track_w / 10) - 2)
                bh = int(cnt / max_cnt * bar_h)
                if bh > 0:
                    col = C_ACCENT2 if yr <= int(self.year_cursor) else C_BG3
                    pygame.draw.rect(self.screen, col,
                                     pygame.Rect(bx, bar_top + bar_h - bh, bw, bh), border_radius=1)

    def _draw_tooltip(self, item: Item) -> None:
        sx, sy = self.vp.w2s(item.x, item.y)
        lines = [
            item.title[:55] + ("…" if len(item.title) > 55 else ""),
            f"{item.platform}  ·  {item.year}",
        ]
        if item.views > 0:
            lines.append(f"Views: {item.views:,.0f}  Likes: {item.likes:,.0f}")
        lines.append(CLUSTER_LABEL[item.cluster % len(CLUSTER_LABEL)])

        pad = 8
        max_w = max(self.font_sm.get_rect(l).width for l in lines) + pad*2
        th = len(lines) * 16 + pad*2
        tx = min(int(sx) + 14, W - max_w - 4)
        ty = min(int(sy) - th//2, H - TIMELINE_H - th - 4)
        ty = max(CY0 + 4, ty)

        _draw_rounded_rect(self.screen, pygame.Rect(tx, ty, max_w, th), C_BG2, radius=6, alpha=230)
        pygame.draw.rect(self.screen, item.color, pygame.Rect(tx, ty, 3, th), border_radius=2)
        for li, line in enumerate(lines):
            col = C_TEXT if li == 0 else C_MUTED
            _draw_text(self.screen, self.font_sm, line, (tx + pad + 3, ty + pad + li * 16), color=col)

    def _draw_detail_panel(self, item: Item) -> None:
        pw, ph = 340, 220
        px = W - pw - 8
        py = HEADER_H + 8
        _draw_rounded_rect(self.screen, pygame.Rect(px, py, pw, ph), C_BG2, radius=10, alpha=245)
        pygame.draw.rect(self.screen, item.color,
                         pygame.Rect(px, py, 4, ph), border_radius=2)
        pygame.draw.rect(self.screen, C_BG3,
                         pygame.Rect(px, py, pw, ph), 1, border_radius=10)

        y = py + 12
        # Title (wrap)
        words = item.title.split()
        line, lines_out = "", []
        for w in words:
            test = (line + " " + w).strip()
            if self.font_md.get_rect(test).width > pw - 24:
                if line: lines_out.append(line)
                line = w
            else:
                line = test
        if line: lines_out.append(line)
        for ln in lines_out[:3]:
            _draw_text(self.screen, self.font_md, ln, (px+12, y), color=C_TEXT); y += 18
        y += 4

        col = PLATFORM_COLOR.get(item.platform, C_MUTED)
        plat_name = {"gdelt_news":"News (GDELT)","youtube":"YouTube","podcast":"Podcast","film_tv":"Film/TV"}.get(item.platform, item.platform)
        _draw_text(self.screen, self.font_sm, f"◈ {plat_name}", (px+12, y), color=col)
        _draw_text(self.screen, self.font_sm, str(item.year), (px+pw-12, y), color=C_MUTED, anchor="topright")
        y += 18

        clab = CLUSTER_LABEL[item.cluster % len(CLUSTER_LABEL)]
        ccol = CLUSTER_COLOR[item.cluster % len(CLUSTER_COLOR)]
        _draw_text(self.screen, self.font_sm, f"◆ {clab}", (px+12, y), color=ccol); y += 18

        if item.views > 0:
            _draw_text(self.screen, self.font_sm, f"Views: {item.views:,.0f}", (px+12, y), color=C_MUTED); y+=16
            _draw_text(self.screen, self.font_sm, f"Likes: {item.likes:,.0f}  Comments: {item.comments:,.0f}", (px+12, y), color=C_MUTED); y+=16
        else:
            _draw_text(self.screen, self.font_sm, f"Rank proxy: {item.rank:.1f}", (px+12, y), color=C_MUTED); y+=16

        y += 4
        _draw_text(self.screen, self.font_sm, "Double-click node to open URL", (px+12, y), color=C_BG3)

        # Close X
        _draw_text(self.screen, self.font_sm, "✕", (px+pw-12, py+8), color=C_MUTED, anchor="topright")

    def _draw_loading_overlay(self) -> None:
        if not self.dm.loading and self.dm.count > 0:
            return
        msg = self.dm.status
        if not self.dm.loading and self.dm.count == 0:
            msg = "Starting up… fetching real data from GDELT & YouTube"

        # Spinner
        t = time.time()
        cx, cy = W//2, H//2
        for i in range(12):
            angle = (i / 12) * math.tau + t * 3
            a = int(255 * (i / 12))
            r = 26
            dx = int(r * math.cos(angle))
            dy = int(r * math.sin(angle))
            pygame.draw.circle(self.screen, (*C_ACCENT, a), (cx+dx, cy+dy), 4)

        # Progress bar
        n = self.dm.progress
        target = 1000
        prog = min(1.0, n / target)
        bw = 300
        bx = cx - bw//2
        by = cy + 50
        _draw_rounded_rect(self.screen, pygame.Rect(bx, by, bw, 8), C_BG3, radius=4)
        if prog > 0:
            _draw_rounded_rect(self.screen, pygame.Rect(bx, by, int(bw*prog), 8), C_ACCENT, radius=4)
        _draw_text(self.screen, self.font_md, msg, (cx, by+20), color=C_MUTED, anchor="midtop")
        _draw_text(self.screen, self.font_lg, "SiamQuantum Atlas", (cx, cy-70), color=C_TEXT, anchor="midbottom")
        _draw_text(self.screen, self.font_sm, "Collecting Thai quantum media data · Please wait",
                   (cx, cy-52), color=C_MUTED, anchor="midbottom")


# ──────────────────────────────────────────────────────────────────
# Entry
# ──────────────────────────────────────────────────────────────────
def main() -> None:
    app = App()
    app.run()
    pygame.quit()

if __name__ == "__main__":
    main()
