import json

from siamquantum_atlas.graph.arena3d_exporter import build_arena_session
from siamquantum_atlas.graph.builder import GraphBundle


def test_arena_export_shape() -> None:
    payload = build_arena_session(
        GraphBundle(
            nodes=[{"id": "n1", "name": "Node", "layer": "Articles", "size": 1.0}],
            edges=[{"source": "n1", "target": "n1", "type": "self", "weight": 1.0}],
            layers=[{"name": "Articles"}],
        )
    )
    json.dumps(payload)
    assert payload["nodes"]
    assert payload["edges"]
    assert payload["layers"]
