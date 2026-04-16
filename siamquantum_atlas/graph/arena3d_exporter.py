from __future__ import annotations

from pathlib import Path

from siamquantum_atlas.graph.builder import GraphBundle
from siamquantum_atlas.utils.files import write_json


def build_arena_session(bundle: GraphBundle) -> dict:
    return {
        "name": "SiamQuantum Atlas",
        "scene": {"backgroundColor": "#ffffff", "camera": {"x": 0, "y": 0, "z": 150}},
        "layers": bundle.layers,
        "nodes": bundle.nodes,
        "edges": bundle.edges,
        "directed": True,
        "channels": [{"name": "default", "color": "#999999"}],
    }


def export_arena_session(bundle: GraphBundle, output_path: Path) -> Path:
    write_json(output_path, build_arena_session(bundle))
    return output_path
