from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

import networkx as nx


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def build_concept_graph(db_path: Path) -> tuple[nx.DiGraph, dict[str, str]]:
    """Build directed concept graph from all triplets. Returns (graph, label_map)."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT subject, relation, object FROM triplets").fetchall()
    conn.close()

    label_map: dict[str, str] = {}
    edge_counts: dict[tuple[str, str], int] = {}

    for subj_raw, _rel, obj_raw in rows:
        sk = _norm((subj_raw or "").strip())
        ok = _norm((obj_raw or "").strip())
        if len(sk) < 2 or len(ok) < 2 or sk == ok:
            continue
        label_map.setdefault(sk, (subj_raw or "").strip())
        label_map.setdefault(ok, (obj_raw or "").strip())
        edge_counts[(sk, ok)] = edge_counts.get((sk, ok), 0) + 1

    G: nx.DiGraph = nx.DiGraph()
    G.add_nodes_from(label_map.keys())
    for (sk, ok), w in edge_counts.items():
        G.add_edge(sk, ok, weight=w)

    return G, label_map


_HUB_PATTERNS: list[tuple[str, str]] = [
    ("quantum computing", "computing"),
    ("quantum", "quantum"),
    ("thailand", "geography"),
    ("thai", "geography"),
    ("cryptography", "security"),
    ("algorithm", "computing"),
    ("physics", "physics"),
    ("entanglement", "physics"),
    ("technology", "technology"),
    ("research", "research"),
    ("university", "institution"),
    ("government", "institution"),
    ("minister", "institution"),
    ("nstda", "institution"),
    ("nectec", "institution"),
    ("ibm", "industry"),
    ("google", "industry"),
    ("china", "geography"),
    ("us", "geography"),
    ("communication", "communication"),
    ("network", "communication"),
]


def _hub_role(label: str) -> str:
    lbl = label.lower()
    for pattern, role in _HUB_PATTERNS:
        if pattern in lbl:
            return role
    return "concept"


def _community_summaries(U: nx.Graph, label_map: dict[str, str]) -> list[dict[str, Any]]:
    if U.number_of_nodes() < 10:
        return []
    largest_nodes = max(nx.connected_components(U), key=len, default=set())
    if len(largest_nodes) < 10:
        return []

    largest = U.subgraph(largest_nodes).copy()
    communities = list(nx.algorithms.community.greedy_modularity_communities(largest))
    summaries: list[dict[str, Any]] = []
    for community in sorted(communities, key=len, reverse=True)[:5]:
        sub = largest.subgraph(community)
        degree = dict(sub.degree())
        hub_id = max(degree, key=degree.get) if degree else ""
        summaries.append({
            "size": len(community),
            "hub": label_map.get(hub_id, hub_id),
            "hub_role": _hub_role(label_map.get(hub_id, hub_id)),
        })
    return summaries


def compute_metrics(db_path: Path) -> dict[str, Any]:
    G, label_map = build_concept_graph(db_path)

    # Degree centrality (undirected view for ranking)
    U = G.to_undirected()
    deg_cent = nx.degree_centrality(U)

    # Betweenness on largest component only (full graph too slow for all)
    components = sorted(nx.weakly_connected_components(G), key=len, reverse=True)
    largest = G.subgraph(components[0]).to_undirected() if components else U
    bet_cent = nx.betweenness_centrality(largest, normalized=True, endpoints=False)

    top_degree = sorted(deg_cent.items(), key=lambda x: -x[1])[:20]
    top_bet = sorted(bet_cent.items(), key=lambda x: -x[1])[:20]

    # Component summaries: top 10 components with their top-degree node
    component_summaries = []
    for comp in components[:10]:
        sub = U.subgraph(comp)
        sub_deg = dict(sub.degree())
        top_node = max(sub_deg, key=lambda n: sub_deg[n]) if sub_deg else ""
        component_summaries.append({
            "size": len(comp),
            "hub": label_map.get(top_node, top_node),
            "hub_role": _hub_role(label_map.get(top_node, top_node)),
        })

    community_summaries = _community_summaries(U, label_map)
    top_degree_rows = [
        {
            "id": k,
            "label": label_map.get(k, k),
            "score": round(v, 6),
            "hub_role": _hub_role(label_map.get(k, k)),
        }
        for k, v in top_degree
    ]
    top_bet_rows = [
        {
            "id": k,
            "label": label_map.get(k, k),
            "score": round(v, 6),
            "hub_role": _hub_role(label_map.get(k, k)),
        }
        for k, v in top_bet
    ]

    hub_interpretation = {
        "degree_hub": top_degree_rows[0] if top_degree_rows else None,
        "broker_hub": top_bet_rows[0] if top_bet_rows else None,
        "note": (
            "Degree hubs capture the most connected concepts; betweenness hubs capture bridge concepts "
            "linking otherwise separate topic clusters."
        ),
    }

    return {
        "nodes": G.number_of_nodes(),
        "links": G.number_of_edges(),
        "components": len(components),
        "largest_component_size": len(components[0]) if components else 0,
        "component_summaries": component_summaries,
        "community_summaries": community_summaries,
        "hub_interpretation": hub_interpretation,
        "top_degree": top_degree_rows,
        "top_betweenness": top_bet_rows,
    }
