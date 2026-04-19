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


def backfill_geo(db_path: Path) -> dict[str, int]:
    """
    GeoIP lookup for GDELT sources missing geo rows. Skips YouTube.
    Returns counts: {success, failure, skipped_youtube}.
    """
    from siamquantum.services.geoip import lookup
    from siamquantum.db.repos import GeoRepo
    from siamquantum.models import GeoCreate

    counts: dict[str, int] = {"success": 0, "failure": 0, "skipped_youtube": 0}

    with get_connection(db_path) as conn:
        pending = SourceRepo(conn).list_missing_geo()

    for source in pending:
        if source.platform != "gdelt":
            counts["skipped_youtube"] += 1
            continue

        result = lookup(source.url)
        if result:
            with get_connection(db_path) as conn:
                GeoRepo(conn).upsert(
                    GeoCreate(
                        source_id=source.id,
                        ip=result.ip,
                        lat=result.lat,
                        lng=result.lng,
                        city=result.city,
                        region=result.region,
                        isp=result.isp,
                        asn_org=result.asn_org,
                        is_cdn_resolved=result.is_cdn_resolved,
                    )
                )
            counts["success"] += 1
        else:
            counts["failure"] += 1

    return counts


def backfill_asn(db_path: Path) -> dict[str, int]:
    """
    Populate asn_org / is_cdn_resolved for existing geo rows where is_cdn_resolved IS NULL.
    Reads ip from geo table directly — no DNS re-resolution needed.
    Returns counts: {updated, skipped_no_ip, skipped_no_asn_db}.
    """
    from siamquantum.services.geoip import lookup_asn, _get_asn_reader
    from siamquantum.db.repos import GeoRepo

    counts: dict[str, int] = {"updated": 0, "skipped_no_ip": 0, "skipped_no_asn_db": 0}

    if not _get_asn_reader():
        with get_connection(db_path) as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM geo WHERE is_cdn_resolved IS NULL"
            ).fetchone()[0]
        counts["skipped_no_asn_db"] = total
        return counts

    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT source_id, ip FROM geo WHERE is_cdn_resolved IS NULL"
        ).fetchall()

    for row in rows:
        source_id: int = row["source_id"]
        ip: str | None = row["ip"]
        if not ip:
            counts["skipped_no_ip"] += 1
            continue

        asn_org, is_cdn = lookup_asn(ip)
        with get_connection(db_path) as conn:
            conn.execute(
                "UPDATE geo SET asn_org = ?, is_cdn_resolved = ? WHERE source_id = ?",
                (asn_org, is_cdn, source_id),
            )
            conn.commit()
        counts["updated"] += 1

    return counts
