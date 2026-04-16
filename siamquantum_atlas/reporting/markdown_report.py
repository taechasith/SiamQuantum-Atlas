from __future__ import annotations

from collections import Counter
from pathlib import Path

from siamquantum_atlas.db.models import ItemClassification, MediaItem


def write_markdown_report(items: list[MediaItem], classifications: dict[int, ItemClassification], output_path: Path) -> Path:
    topic_counts = Counter(classifications[item.id].main_topic for item in items if item.id in classifications)
    lines = ["# SiamQuantum-Atlas Report", "", f"Total media items: {len(items)}", "", "## Top Topics"]
    for topic, count in topic_counts.most_common(10):
        lines.append(f"- {topic}: {count}")
    lines.extend(["", "## Item Summaries"])
    for item in items:
        classification = classifications.get(item.id)
        lines.append(
            f"- {item.title} | {item.platform} | {item.media_type} | "
            f"normalization={classification.normalization_score if classification else 'n/a'} | "
            f"distortion={classification.distortion_risk if classification else 'n/a'}"
        )
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path
