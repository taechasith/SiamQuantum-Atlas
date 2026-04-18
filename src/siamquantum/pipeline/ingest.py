from __future__ import annotations

from datetime import datetime
from pathlib import Path

from siamquantum.db.repos import SourceRepo
from siamquantum.db.session import get_connection
from siamquantum.models import SourceCreate, SourceRaw


def _to_create(raw: SourceRaw) -> SourceCreate:
    return SourceCreate(
        platform=raw.platform,
        url=raw.url,
        title=raw.title,
        raw_text=raw.raw_text,
        published_year=raw.published_year,
        fetched_at=datetime.utcnow(),
        view_count=raw.view_count,
        like_count=raw.like_count,
        comment_count=raw.comment_count,
    )


def write_sources(raws: list[SourceRaw], db_path: Path) -> int:
    """Insert SourceRaw records into DB. Returns count of newly inserted rows."""
    inserted = 0
    with get_connection(db_path) as conn:
        repo = SourceRepo(conn)
        for raw in raws:
            row_id = repo.insert(_to_create(raw))
            if row_id:
                inserted += 1
    return inserted


async def ingest_gdelt_year(year: int, db_path: Path) -> tuple[int, int]:
    """
    Fetch GDELT for `year`, write to DB.
    Returns (fetched_count, inserted_count).
    """
    from siamquantum.services import gdelt

    result = await gdelt.fetch_yearly(year)
    if not result.ok:
        raise RuntimeError(f"GDELT fetch failed: {result.error}")

    raws = [SourceRaw(**item) for item in (result.data or [])]
    inserted = write_sources(raws, db_path)
    return len(raws), inserted


async def ingest_youtube_year(year: int, db_path: Path) -> tuple[int, int]:
    """
    Fetch YouTube for `year`, write to DB.
    Returns (fetched_count, inserted_count).
    """
    from siamquantum.services import youtube

    result = await youtube.fetch_yearly(year)
    if not result.ok:
        raise RuntimeError(f"YouTube fetch failed: {result.error}")

    raws = [SourceRaw(**item) for item in (result.data or [])]
    inserted = write_sources(raws, db_path)
    return len(raws), inserted
