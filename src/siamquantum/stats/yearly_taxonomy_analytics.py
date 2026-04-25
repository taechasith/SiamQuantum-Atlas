from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Callable

import networkx as nx  # type: ignore[import-untyped]
import numpy as np
from numpy.typing import NDArray

from siamquantum.stats.engagement_bootstrap import (
    bootstrap_geometric_mean,
    log_transform_engagement,
    trend_test,
)
from siamquantum.stats.nonparametric import chi2_independence, kruskal_wallis


def _normalize_text(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(str(value).strip().lower().replace("_", " ").split())
    return cleaned or None


def _display_label(value: str) -> str:
    parts = value.replace("_", " ").split()
    if not parts:
        return value
    return " ".join(part.upper() if len(part) <= 3 else part.capitalize() for part in parts)


def topic_label(row: dict[str, Any]) -> str | None:
    area = _normalize_text(row.get("area"))
    if area:
        return area
    domain = _normalize_text(row.get("quantum_domain"))
    if not domain or domain == "not applicable":
        return None
    return domain.replace("quantum ", "quantum ")


_MEDIA_FALLBACKS = {
    "text static": "article",
    "audio": "podcast",
    "video short": "short video",
    "video long": "long-form video",
    "broadcast ott": "broadcast",
    "movie": "film",
    "animation": "animation",
}

_CONTENT_TYPE_FALLBACKS = {
    "academic": "academic paper",
    "news": "article",
    "educational": "course",
    "entertainment": "entertainment feature",
}

_PRODUCTION_TYPE_FALLBACKS = {
    "state research": "institutional report",
    "university": "academic paper",
    "corporate media": "media production",
    "independent": "creator production",
}


def production_label(row: dict[str, Any]) -> str | None:
    detail = _normalize_text(row.get("media_format_detail"))
    if detail:
        if "podcast" in detail:
            return "podcast"
        if "vlog" in detail:
            return "vlog"
        if "course" in detail or "lecture" in detail:
            return "course"
        if "paper" in detail or "journal" in detail:
            return "academic paper"
        if "film" in detail or "documentary" in detail:
            return "film"
        if "game" in detail:
            return "game"
        if "digital art" in detail or "artwork" in detail:
            return "digital art"
        if "article" in detail or "news" in detail:
            return "article"
        return detail

    media_format = _normalize_text(row.get("media_format"))
    if media_format and media_format in _MEDIA_FALLBACKS:
        return _MEDIA_FALLBACKS[media_format]

    content_type = _normalize_text(row.get("content_type"))
    if content_type and content_type in _CONTENT_TYPE_FALLBACKS:
        return _CONTENT_TYPE_FALLBACKS[content_type]

    production_type = _normalize_text(row.get("production_type"))
    if production_type and production_type in _PRODUCTION_TYPE_FALLBACKS:
        return _PRODUCTION_TYPE_FALLBACKS[production_type]
    return production_type


def _sorted_years(rows: list[dict[str, Any]]) -> list[int]:
    years = {int(row["published_year"]) for row in rows if row.get("published_year")}
    return sorted(year for year in years if year > 0)


def _stable_labels(counter: Counter[str], *, top_n: int) -> list[str]:
    ranked = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    stable = [label for label, count in ranked if count >= 2][:top_n]
    if stable:
        return stable
    return [label for label, _count in ranked[: min(top_n, len(ranked))]]


def _log_array(values: list[float]) -> NDArray[np.float64]:
    return log_transform_engagement(np.array(values, dtype=float))


def _community_payload(graph: nx.Graph, node_data: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if graph.number_of_nodes() == 0 or graph.number_of_edges() == 0:
        return []
    communities = list(nx.algorithms.community.greedy_modularity_communities(graph, weight="weight"))
    payload: list[dict[str, Any]] = []
    for index, community in enumerate(sorted(communities, key=len, reverse=True)[:5]):
        labels = [node_data[node]["label"] for node in community if node_data[node]["kind"] != "year"]
        payload.append(
            {
                "community": index,
                "size": len(community),
                "labels": labels[:6],
            }
        )
    return payload


def _build_graph(
    years: list[int],
    labels: list[str],
    yearly_counts: dict[int, dict[str, int]],
    yearly_stats: dict[int, dict[str, dict[str, Any]]],
    *,
    node_prefix: str,
) -> dict[str, Any]:
    graph = nx.Graph()
    node_data: dict[str, dict[str, Any]] = {}

    for year in years:
        node_id = f"year:{year}"
        count_sum = sum(yearly_counts.get(year, {}).values())
        node_data[node_id] = {
            "id": node_id,
            "label": str(year),
            "kind": "year",
            "value": max(count_sum, 1),
            "count": count_sum,
        }
        graph.add_node(node_id)

    for label in labels:
        node_id = f"{node_prefix}:{label}"
        total = sum(yearly_counts.get(year, {}).get(label, 0) for year in years)
        node_data[node_id] = {
            "id": node_id,
            "label": _display_label(label),
            "kind": node_prefix,
            "value": max(total, 1),
            "count": total,
        }
        graph.add_node(node_id)

    links: list[dict[str, Any]] = []
    for year in years:
        for label in labels:
            count = int(yearly_counts.get(year, {}).get(label, 0))
            if count <= 0:
                continue
            stat = yearly_stats.get(year, {}).get(label) or {}
            source = f"year:{year}"
            target = f"{node_prefix}:{label}"
            graph.add_edge(source, target, weight=count)
            links.append(
                {
                    "source": source,
                    "target": target,
                    "weight": count,
                    "geo_mean": stat.get("geo_mean", 0.0),
                    "ci_low": stat.get("ci_low", 0.0),
                    "ci_high": stat.get("ci_high", 0.0),
                    "n": stat.get("n", count),
                }
            )

    communities = list(nx.algorithms.community.greedy_modularity_communities(graph, weight="weight")) if graph.number_of_edges() else []
    for community_index, community in enumerate(communities):
        for node_id in community:
            node_data[node_id]["community"] = community_index
    for node_id in node_data:
        node_data[node_id].setdefault("community", 0)

    return {
        "nodes": list(node_data.values()),
        "links": links,
        "community_summaries": _community_payload(graph, node_data),
    }


def _axis_payload(
    rows: list[dict[str, Any]],
    label_fn: Callable[[dict[str, Any]], str | None],
    *,
    top_n: int,
    node_prefix: str,
    axis_name: str,
) -> dict[str, Any]:
    labelled_rows: list[dict[str, Any]] = []
    label_counter: Counter[str] = Counter()
    for row in rows:
        label = label_fn(row)
        year = int(row.get("published_year") or 0)
        if not label or year <= 0:
            continue
        row_with_label = {**row, "_label": label}
        labelled_rows.append(row_with_label)
        label_counter[label] += 1

    years = _sorted_years(labelled_rows)
    labels = _stable_labels(label_counter, top_n=top_n)

    yearly_counts: dict[int, dict[str, int]] = defaultdict(dict)
    yearly_values: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    all_values_by_label: dict[str, list[float]] = defaultdict(list)
    trends: list[dict[str, Any]] = []

    for row in labelled_rows:
        label = row["_label"]
        if label not in labels:
            continue
        year = int(row["published_year"])
        value = float(row.get("view_count") or 0)
        yearly_values[year][label].append(value)
        all_values_by_label[label].append(value)

    yearly_stats: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
    series: list[dict[str, Any]] = []
    for label in labels:
        by_year_stats: dict[str, dict[str, Any]] = {}
        trend_years: list[int] = []
        trend_logs: list[NDArray[np.float64]] = []
        total_count = 0
        for year in years:
            values = yearly_values.get(year, {}).get(label, [])
            yearly_counts[year][label] = len(values)
            total_count += len(values)
            if not values:
                continue
            log_values = _log_array(values)
            stats = bootstrap_geometric_mean(log_values, n_resamples=2_000)
            yearly_stats[year][label] = stats
            by_year_stats[str(year)] = {
                "count": len(values),
                "geo_mean": stats["geo_mean"],
                "ci_low": stats["ci_low"],
                "ci_high": stats["ci_high"],
            }
            if len(values) >= 2:
                trend_years.append(year)
                trend_logs.append(log_values)

        trend = {"note": "insufficient_years", "label": label}
        if len(trend_years) >= 3:
            trend = trend_test(trend_years, trend_logs)
            trend["label"] = label
            trends.append(trend)

        series.append(
            {
                "label": _display_label(label),
                "key": label,
                "total_count": total_count,
                "years": by_year_stats,
                "trend": trend,
            }
        )

    overall_groups = {
        label: _log_array(values)
        for label, values in all_values_by_label.items()
        if label in labels and len(values) >= 2
    }
    overall_kw = kruskal_wallis(overall_groups)

    yearwise_tests: list[dict[str, Any]] = []
    for year in years:
        groups = {
            label: _log_array(values)
            for label, values in yearly_values.get(year, {}).items()
            if label in labels and len(values) >= 2
        }
        test = kruskal_wallis(groups)
        test["year"] = year
        test["groups_tested"] = sorted(groups.keys())
        yearwise_tests.append(test)

    contingency = {
        (str(year), label): int(yearly_counts.get(year, {}).get(label, 0))
        for year in years
        for label in labels
    }
    chi2 = chi2_independence(contingency, [str(year) for year in years], labels)

    strongest_trend = max(
        (trend for trend in trends if trend.get("mannkendall_tau") is not None),
        key=lambda item: abs(float(item.get("mannkendall_tau", 0.0))),
        default=None,
    )

    return {
        "axis": axis_name,
        "labels": [{"key": label, "label": _display_label(label), "count": label_counter[label]} for label in labels],
        "years": [str(year) for year in years],
        "series": series,
        "tests": {
            "overall_kruskal_wallis": overall_kw,
            "yearly_kruskal_wallis": yearwise_tests,
            "year_x_label_chi2": chi2,
            "strongest_trend": strongest_trend,
        },
        "graph": _build_graph(years, labels, yearly_counts, yearly_stats, node_prefix=node_prefix),
    }


def build_yearly_taxonomy_analytics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "topics": _axis_payload(rows, topic_label, top_n=8, node_prefix="topic", axis_name="topic"),
        "productions": _axis_payload(rows, production_label, top_n=8, node_prefix="production", axis_name="production"),
        "method_note": (
            "Yearly engagement uses bootstrap geometric means on log1p(view_count). "
            "Group validation uses Kruskal-Wallis for engagement differences, chi-square for year-by-category composition, "
            "and Mann-Kendall plus Spearman for label-level trends when at least three years have stable observations."
        ),
    }
