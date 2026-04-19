"""Phase 4 audit Step 3: dedup analysis on 10 sources."""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from siamquantum.db.session import get_connection, db_path_from_url
from siamquantum.config import settings
from siamquantum.services.dedup import _make_vectorizer, _LOW_THRESHOLD, _HIGH_THRESHOLD

SELECTED_IDS = [27, 16, 28, 12, 44, 175, 170, 207, 215, 201]

db_path = db_path_from_url(settings.database_url)
with get_connection(db_path) as conn:
    rows = {
        r["id"]: r for r in conn.execute(
            f"SELECT id, platform, title, raw_text FROM sources WHERE id IN ({','.join('?'*len(SELECTED_IDS))})",
            SELECTED_IDS,
        ).fetchall()
    }

ordered = [rows[i] for i in SELECTED_IDS]
texts = [(r["raw_text"] or r["title"] or "").strip() for r in ordered]
ids = [r["id"] for r in ordered]
titles = [(r["title"] or "")[:60] for r in ordered]

vectorizer = _make_vectorizer()
matrix = vectorizer.fit_transform(texts)
sims: np.ndarray[tuple[int, int], np.dtype[np.float64]] = cosine_similarity(matrix)

print("=== Pairwise cosine similarities (non-trivial pairs) ===")
pairs_low = 0
pairs_ambiguous = 0
pairs_high = 0
merges = []

for i in range(len(ids)):
    for j in range(i + 1, len(ids)):
        score = float(sims[i, j])
        if score <= 0.05:
            continue  # trivially different, skip printing
        zone = "different" if score <= _LOW_THRESHOLD else ("ambiguous" if score <= _HIGH_THRESHOLD else "DUPLICATE")
        if score <= _LOW_THRESHOLD:
            pairs_low += 1
        elif score <= _HIGH_THRESHOLD:
            pairs_ambiguous += 1
        else:
            pairs_high += 1
            merges.append((ids[i], ids[j], score, titles[i], titles[j]))
        print(f"  [{ids[i]} x {ids[j]}] cosine={score:.3f} zone={zone}")
        print(f"    A: {repr(titles[i])}")
        print(f"    B: {repr(titles[j])}")

print(f"\n=== SUMMARY ===")
print(f"  pairs > 0.05:     {pairs_low + pairs_ambiguous + pairs_high}")
print(f"  different (<0.6): {pairs_low}")
print(f"  ambiguous [0.6-0.85] (Claude tiebreak): {pairs_ambiguous}")
print(f"  auto-duplicate (>0.85): {pairs_high}")
if merges:
    print(f"  merges:")
    for a, b, s, ta, tb in merges:
        print(f"    id={a} + id={b} cosine={s:.3f}")
        print(f"      keep: {repr(ta)}")
        print(f"      drop: {repr(tb)}")
else:
    print("  merges: none")
