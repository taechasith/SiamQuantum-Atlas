from __future__ import annotations

import csv
from pathlib import Path

from siamquantum_atlas.db.models import ItemClassification, MediaItem


def write_csv_report(items: list[MediaItem], classifications: dict[int, ItemClassification], output_path: Path) -> Path:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "title", "platform", "media_type", "main_topic", "normalization_score", "distortion_risk"])
        for item in items:
            classification = classifications.get(item.id)
            writer.writerow([item.id, item.title, item.platform, item.media_type, classification.main_topic if classification else "", classification.normalization_score if classification else "", classification.distortion_risk if classification else ""])
    return output_path
