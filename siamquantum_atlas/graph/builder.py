from __future__ import annotations

from dataclasses import dataclass

from siamquantum_atlas.db.models import Cluster, ClusterMembership, ItemClassification, MediaItem


@dataclass(slots=True)
class GraphBundle:
    nodes: list[dict]
    edges: list[dict]
    layers: list[dict]


def build_graph_payload(
    items: list[MediaItem],
    classifications: dict[int, ItemClassification],
    memberships: list[ClusterMembership],
    clusters: dict[int, Cluster],
) -> GraphBundle:
    nodes: list[dict] = []
    edges: list[dict] = []
    layers = [{"name": name} for name in ["Articles", "Videos", "Podcasts", "Films_TV", "Topics", "Frames", "Platforms", "Time", "Clusters"]]
    seen_nodes: set[str] = set()

    def add_node(node: dict) -> None:
        if node["id"] not in seen_nodes:
            seen_nodes.add(node["id"])
            nodes.append(node)

    for item in items:
        item_id = f"item:{item.id}"
        layer = {"article": "Articles", "video": "Videos", "podcast": "Podcasts", "film_tv": "Films_TV"}.get(item.media_type, "Articles")
        size = 1.0
        if item.engagement_metrics:
            metric = item.engagement_metrics[-1]
            size += ((metric.views or 0) / 100000) + ((metric.popularity_proxy or 0) / 100)
        add_node({"id": item_id, "name": item.title, "layer": layer, "size": round(size, 3), "color": "#2E86AB"})
        platform_id = f"platform:{item.platform}"
        add_node({"id": platform_id, "name": item.platform, "layer": "Platforms", "size": 1.8, "color": "#F18F01"})
        edges.append({"source": item_id, "target": platform_id, "type": "published_on_platform", "weight": 1.0})
        time_label = str(item.published_at.year) if item.published_at else "unknown"
        time_id = f"time:{time_label}"
        add_node({"id": time_id, "name": time_label, "layer": "Time", "size": 1.5, "color": "#C73E1D"})
        edges.append({"source": item_id, "target": time_id, "type": "occurs_in_time_period", "weight": 1.0})
        classification = classifications.get(item.id)
        if classification:
            topic_id = f"topic:{classification.main_topic}"
            add_node({"id": topic_id, "name": classification.main_topic, "layer": "Topics", "size": 1.8, "color": "#6A994E"})
            edges.append({"source": item_id, "target": topic_id, "type": "discusses_topic", "weight": 1.0})
            for frame in classification.secondary_topics_json:
                frame_id = f"frame:{frame}"
                add_node({"id": frame_id, "name": frame, "layer": "Frames", "size": 1.5, "color": "#7B2CBF"})
                edges.append({"source": item_id, "target": frame_id, "type": "belongs_to_frame", "weight": 0.8})
    for membership in memberships:
        cluster = clusters[membership.cluster_id]
        cluster_id = f"cluster:{cluster.id}"
        add_node({"id": cluster_id, "name": cluster.label, "layer": "Clusters", "size": 2.2, "color": "#3A0CA3"})
        edges.append({"source": f"item:{membership.media_item_id}", "target": cluster_id, "type": "cluster_member_of", "weight": membership.membership_strength})
    return GraphBundle(nodes=nodes, edges=edges, layers=layers)
