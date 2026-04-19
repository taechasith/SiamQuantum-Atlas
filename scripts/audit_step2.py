"""Phase 4 audit: run NLP on 10 sources, track real API costs."""
from __future__ import annotations
import sys, json
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

from siamquantum.db.session import get_connection, db_path_from_url
from siamquantum.config import settings

db_path = db_path_from_url(settings.database_url)

with get_connection(db_path) as conn:
    gdelt = conn.execute(
        'SELECT id, platform, url, title, raw_text FROM sources WHERE platform="gdelt" ORDER BY RANDOM() LIMIT 5'
    ).fetchall()
    youtube = conn.execute(
        'SELECT id, platform, url, title, raw_text FROM sources WHERE platform="youtube" ORDER BY RANDOM() LIMIT 5'
    ).fetchall()

selected = list(gdelt) + list(youtube)
ids = [r["id"] for r in selected]

print("=== Selected 10 sources ===")
for r in selected:
    text = (r["raw_text"] or r["title"] or "").strip()
    print(f"  id={r['id']} platform={r['platform']} text_len={len(text)} title={repr((r['title'] or '')[:70])}")

print(f"\nIDs: {ids}")
