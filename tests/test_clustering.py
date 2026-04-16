from siamquantum_atlas.clustering.graph_clusters import build_similarity_graph
from siamquantum_atlas.clustering.semantic import semantic_cluster


def test_semantic_and_graph_clustering() -> None:
    vectors = [[1.0, 0.0, 0.0], [0.9, 0.1, 0.0], [0.0, 1.0, 0.0]]
    semantic = semantic_cluster(vectors, k=2)
    graph = build_similarity_graph(vectors, threshold=0.8)
    assert len(semantic.labels) == 3
    assert graph.communities
