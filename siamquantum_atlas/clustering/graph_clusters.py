from __future__ import annotations

import math
from dataclasses import dataclass, field

import networkx as nx


# ── Helpers ────────────────────────────────────────────────────────────────────

def cosine_similarity(a: list[float], b: list[float]) -> float:
    numerator = sum(x * y for x, y in zip(a, b))
    denom_a   = math.sqrt(sum(x * x for x in a)) or 1.0
    denom_b   = math.sqrt(sum(y * y for y in b)) or 1.0
    return numerator / (denom_a * denom_b)


# ── Result type ────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class GraphClusterResult:
    graph: nx.Graph
    communities: list[list[int]]           # Louvain community membership lists
    louvain_labels: dict[int, int]         # node index → community id
    constraint: dict[int, float]           # Burt's structural constraint per node


# ── Main function ──────────────────────────────────────────────────────────────

def build_similarity_graph(
    vectors: list[list[float]],
    threshold: float = 0.45,
) -> GraphClusterResult:
    """Build a cosine-similarity graph, detect Louvain communities, and compute
    Burt's structural constraint (brokerage measure) for every node.

    Parameters
    ----------
    vectors:   Per-node embedding vectors.
    threshold: Minimum cosine similarity for an edge to be created.

    Returns
    -------
    GraphClusterResult with:
    - communities       – Louvain partitions (each a list of node indices)
    - louvain_labels    – node → community id mapping
    - constraint        – Burt's constraint C_i ∈ (0, 1]; lower = better broker
    """
    n = len(vectors)
    graph = nx.Graph()
    graph.add_nodes_from(range(n))

    for i in range(n):
        for j in range(i + 1, n):
            score = cosine_similarity(vectors[i], vectors[j])
            if score >= threshold:
                graph.add_edge(i, j, weight=round(score, 4))

    # ── Louvain modularity community detection ─────────────────────────────────
    try:
        parts = nx.community.louvain_communities(graph, weight="weight", seed=42)
        communities = [sorted(list(c)) for c in parts]
    except Exception:
        # Fallback: connected components
        communities = [sorted(list(c)) for c in nx.connected_components(graph)]

    louvain_labels: dict[int, int] = {}
    for cid, members in enumerate(communities):
        for node in members:
            louvain_labels[node] = cid

    # ── Burt's structural constraint ───────────────────────────────────────────
    # nx.constraint returns C_i where high values = redundant contacts = poor broker
    try:
        raw_constraint = nx.constraint(graph, weight="weight")
        # Isolated nodes (no edges) get constraint = 1.0 by convention
        constraint: dict[int, float] = {
            node: round(float(raw_constraint.get(node, 1.0)), 4)
            for node in graph.nodes()
        }
    except Exception:
        constraint = {node: 1.0 for node in graph.nodes()}

    return GraphClusterResult(
        graph=graph,
        communities=communities,
        louvain_labels=louvain_labels,
        constraint=constraint,
    )
