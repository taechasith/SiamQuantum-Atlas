from __future__ import annotations

from siamquantum_atlas.llm.schemas import TripletRecord


def heuristic_triplets(topic: str, platform: str) -> list[TripletRecord]:
    return [TripletRecord(subject=topic, predicate="published_on_platform", object=platform, confidence=0.6)]
