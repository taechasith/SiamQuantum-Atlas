"""Post-backfill audit."""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

from siamquantum.db.session import get_connection, db_path_from_url
from siamquantum.config import settings

db_path = db_path_from_url(settings.database_url)
with get_connection(db_path) as conn:
    missing = conn.execute('''
        SELECT s.id, s.platform, s.title, s.raw_text
        FROM sources s LEFT JOIN entities e ON s.id = e.source_id
        WHERE e.source_id IS NULL
    ''').fetchall()
    print('Missing entity rows:')
    for r in missing:
        text = (r['raw_text'] or r['title'] or '').strip()
        print(f'  id={r["id"]} platform={r["platform"]} text_len={len(text)} title={repr((r["title"] or "")[:80])}')

    print()
    ct = conn.execute('SELECT content_type, COUNT(*) as n FROM entities GROUP BY content_type').fetchall()
    pt = conn.execute('SELECT production_type, COUNT(*) as n FROM entities GROUP BY production_type').fetchall()
    el = conn.execute('SELECT engagement_level, COUNT(*) as n FROM entities GROUP BY engagement_level').fetchall()
    print('content_type:', {r["content_type"]: r["n"] for r in ct})
    print('production_type:', {r["production_type"]: r["n"] for r in pt})
    print('engagement_level:', {r["engagement_level"]: r["n"] for r in el})

    total_trip = conn.execute('SELECT COUNT(*) FROM triplets').fetchone()[0]
    avg_trip = conn.execute('SELECT AVG(cnt) FROM (SELECT COUNT(*) as cnt FROM triplets GROUP BY source_id)').fetchone()[0]
    zero_trip = conn.execute('''
        SELECT COUNT(*) FROM sources s
        LEFT JOIN triplets t ON s.id = t.source_id
        WHERE t.source_id IS NULL
    ''').fetchone()[0]
    print(f'\ntriplets total={total_trip}  avg_per_source={avg_trip:.1f}  sources_with_zero_triplets={zero_trip}')
    abstention_rate = zero_trip / 147 * 100
    print(f'abstention rate (no triplets extracted): {abstention_rate:.1f}%')
