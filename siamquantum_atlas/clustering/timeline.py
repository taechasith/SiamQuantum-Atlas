from __future__ import annotations

from collections import Counter
from datetime import datetime


def summarize_timeline(dates: list[datetime | None]) -> dict[str, int]:
    return dict(Counter(str(date.year) if date else "unknown" for date in dates))
