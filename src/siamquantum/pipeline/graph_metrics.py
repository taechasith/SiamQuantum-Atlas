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

    return {
        "nodes": G.number_of_nodes(),
        "links": G.number_of_edges(),
        "components": len(components),
        "largest_component_size": len(components[0]) if components else 0,
        "top_degree": [
            {"id": k, "label": label_map.get(k, k), "score": round(v, 6)}
            for k, v in top_degree
        ],
        "top_betweenness": [
            {"id": k, "label": label_map.get(k, k), "score": round(v, 6)}
            for k, v in top_bet
        ],
    }
