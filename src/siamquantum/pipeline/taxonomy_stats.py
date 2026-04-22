from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

from siamquantum.db.repos import StatsCacheRepo
from siamquantum.db.session import get_connection
from siamquantum.stats.engagement_bootstrap import bootstrap_geometric_mean, log_transform_engagement, trend_test
from siamquantum.stats.nonparametric import chi2_independence, kruskal_wallis, mann_whitney


def _fetch_rows(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT s.view_count, s.published_year,
               e.media_format, e.user_intent, e.thai_cultural_angle
        FROM sources s
        JOIN entities e ON e.source_id = s.id
        WHERE e.media_format IS NOT NULL AND e.user_intent IS NOT NULL
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _group_log_views(rows: list[dict[str, Any]], key: str) -> dict[str, np.ndarray]:
    groups: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        val = r.get(key) or "unknown"
        groups[val].append(float(r["view_count"] or 0))
    return {k: log_transform_engagement(np.array(v, dtype=float)) for k, v in groups.items()}


def _summarise_groups(groups: dict[str, np.ndarray]) -> list[dict[str, Any]]:
    out = []
    for label, log_views in groups.items():
        bs = bootstrap_geometric_mean(log_views, n_resamples=5_000)
        bs["label"] = label
        out.append(bs)
    return sorted(out, key=lambda x: -x["geo_mean"])


def _year_trend(rows: list[dict[str, Any]], key: str, value: str) -> dict[str, Any]:
    by_year: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        if (r.get(key) or "unknown") == value:
            by_year[int(r["published_year"] or 0)].append(float(r["view_count"] or 0))
    years = sorted(y for y in by_year if y > 0)
    log_per_year = [log_transform_engagement(np.array(by_year[y], dtype=float)) for y in years]
    if len(years) < 3:
        return {"note": "insufficient_years", "label": value}
    result = trend_test(years, log_per_year)
    result["label"] = value
    result["years"] = years
    return result


def run_taxonomy_stats(db_path: Path) -> dict[str, int]:
    rows = _fetch_rows(db_path)
    if not rows:
        return {"keys_written": 0}

    with get_connection(db_path) as conn:
        cache = StatsCacheRepo(conn)

        # 1. engagement by media_format
        mf_groups = _group_log_views(rows, "media_format")
        mf_summary = _summarise_groups(mf_groups)
        mf_kw = kruskal_wallis({k: v for k, v in mf_groups.items()})
        cache.set("taxonomy:media_format", {"summary": mf_summary, "kruskal_wallis": mf_kw})

        # 2. engagement by user_intent
        ui_groups = _group_log_views(rows, "user_intent")
        ui_summary = _summarise_groups(ui_groups)
        ui_kw = kruskal_wallis({k: v for k, v in ui_groups.items()})
        cache.set("taxonomy:user_intent", {"summary": ui_summary, "kruskal_wallis": ui_kw})

        # 3. thai_cultural_angle null vs non-null (Mann-Whitney)
        thai_yes = log_transform_engagement(np.array(
            [float(r["view_count"] or 0) for r in rows if r.get("thai_cultural_angle")], dtype=float))
        thai_no = log_transform_engagement(np.array(
            [float(r["view_count"] or 0) for r in rows if not r.get("thai_cultural_angle")], dtype=float))
        thai_mw = mann_whitney(thai_yes, thai_no)
        cache.set("taxonomy:thai_cultural_angle", {
            "n_with": int(len(thai_yes)),
            "n_without": int(len(thai_no)),
            "geo_mean_with": bootstrap_geometric_mean(thai_yes, n_resamples=2_000),
            "geo_mean_without": bootstrap_geometric_mean(thai_no, n_resamples=2_000),
            "mann_whitney": thai_mw,
        })

        # 4. chi-square media_format × user_intent
        mf_cats = sorted(set(r["media_format"] for r in rows if r["media_format"]))
        ui_cats = sorted(set(r["user_intent"] for r in rows if r["user_intent"]))
        contingency: dict[tuple[str, str], int] = defaultdict(int)
        for r in rows:
            if r["media_format"] and r["user_intent"]:
                contingency[(r["media_format"], r["user_intent"])] += 1
        chi2_result = chi2_independence(contingency, mf_cats, ui_cats)
        chi2_result["row_cats"] = mf_cats
        chi2_result["col_cats"] = ui_cats
        cache.set("taxonomy:media_x_intent:chi2", chi2_result)

        # 5. year trend: top 3 media_formats
        top_mf = [s["label"] for s in mf_summary[:3]]
        keys_written = 4
        for mf in top_mf:
            trend = _year_trend(rows, "media_format", mf)
            cache.set(f"taxonomy:trend:media_format:{mf}", trend)
            keys_written += 1

        # 6. year trend: top 3 user_intents
        top_ui = [s["label"] for s in ui_summary[:3]]
        for ui in top_ui:
            trend = _year_trend(rows, "user_intent", ui)
            cache.set(f"taxonomy:trend:user_intent:{ui}", trend)
            keys_written += 1

    return {"keys_written": keys_written, "rows_analysed": len(rows)}
