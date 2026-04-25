from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from pathlib import Path

from siamquantum.db.repos import SourceRepo
from siamquantum.db.session import get_connection
from siamquantum.models import SourceCreate, SourceRaw

logger = logging.getLogger(__name__)


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
        channel_id=raw.channel_id,
        channel_title=raw.channel_title,
        channel_country=raw.channel_country,
        channel_default_language=raw.channel_default_language,
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


def _insert_sources(records: list[dict], db_path: Path) -> int:
    """Insert raw dicts (SourceRaw schema) into DB. Public alias for CLI commands."""
    raws = [SourceRaw(**r) for r in records]
    return write_sources(raws, db_path)


async def ingest_gdelt_year(year: int, db_path: Path) -> tuple[int, int]:
    """Fetch GDELT for `year`, write to DB. Returns (fetched, inserted)."""
    return await ingest_gdelt_daterange(date(year, 1, 1), date(year, 12, 31), db_path)


async def ingest_gdelt_daterange(start: date, end: date, db_path: Path) -> tuple[int, int]:
    """Fetch GDELT for [start, end] inclusive, write to DB. Returns (fetched, inserted)."""
    from siamquantum.services import gdelt

    result = await gdelt.fetch_daterange(start, end)
    if not result.ok:
        raise RuntimeError(f"GDELT fetch failed: {result.error}")

    raws = [SourceRaw(**item) for item in (result.data or [])]
    inserted = write_sources(raws, db_path)
    return len(raws), inserted


async def ingest_youtube_year(year: int, db_path: Path) -> tuple[int, int]:
    """Fetch YouTube for `year`, write to DB. Returns (fetched, inserted)."""
    return await ingest_youtube_daterange(date(year, 1, 1), date(year, 12, 31), db_path)


async def ingest_youtube_daterange(start: date, end: date, db_path: Path) -> tuple[int, int]:
    """Fetch YouTube for [start, end] inclusive, write to DB. Returns (fetched, inserted)."""
    from siamquantum.services import youtube

    result = await youtube.fetch_daterange(start, end)
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
    If ISP is missing but ASN organisation is known, sync ISP from ASN for display continuity.
    Returns counts: {updated, isp_synced, skipped_no_ip, skipped_no_asn_db}.
    """
    from siamquantum.services.geoip import lookup_asn, _get_asn_reader

    counts: dict[str, int] = {"updated": 0, "isp_synced": 0, "skipped_no_ip": 0, "skipped_no_asn_db": 0}

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
            before = conn.execute(
                "SELECT isp FROM geo WHERE source_id = ?",
                (source_id,),
            ).fetchone()
            conn.execute(
                """
                UPDATE geo
                SET asn_org = ?,
                    is_cdn_resolved = ?,
                    isp = COALESCE(isp, ?)
                WHERE source_id = ?
                """,
                (asn_org, is_cdn, asn_org, source_id),
            )
            conn.commit()
        if (before["isp"] if before else None) is None and asn_org:
            counts["isp_synced"] += 1
        counts["updated"] += 1

    return counts


async def backfill_channel_metadata(db_path: Path) -> dict[str, int]:
    """
    Populate channel_id/title/country/default_language for existing YouTube rows where
    channel_id IS NULL. Calls videos.list (get channelId+title) then channels.list
    (get country/defaultLanguage). Idempotent — skips rows already populated.
    YouTube quota cost: ~8 (videos.list) + ~3 (channels.list) units total.
    """
    from siamquantum.services.youtube import _fetch_channel_info, _get
    from siamquantum.config import settings
    import httpx

    counts: dict[str, int] = {"updated": 0, "skipped_no_video": 0, "api_errors": 0}

    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT id, url FROM sources WHERE platform='youtube' AND channel_id IS NULL"
        ).fetchall()

    if not rows:
        logger.info("backfill_channel_metadata: nothing to backfill")
        return counts

    # Extract video_ids from URLs
    url_to_id: dict[str, int] = {}
    vid_ids: list[str] = []
    for row in rows:
        src_id: int = row["id"]
        url: str = row["url"]
        vid_id = url.split("?v=")[-1] if "?v=" in url else ""
        if not vid_id:
            counts["skipped_no_video"] += 1
            continue
        url_to_id[vid_id] = src_id
        vid_ids.append(vid_id)

    _VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

    # Batch videos.list to get channelId + channelTitle
    vid_to_channel: dict[str, dict[str, str]] = {}  # vid_id -> {channel_id, channel_title}
    async with httpx.AsyncClient() as client:
        for i in range(0, len(vid_ids), 50):
            batch = vid_ids[i : i + 50]
            params: dict[str, str] = {
                "part": "snippet",
                "id": ",".join(batch),
                "key": settings.youtube_api_key,
            }
            try:
                data = await _get(client, _VIDEOS_URL, params)
                for item in data.get("items") or []:
                    vid = item.get("id") or ""
                    snippet = item.get("snippet") or {}
                    if vid:
                        vid_to_channel[vid] = {
                            "channel_id": snippet.get("channelId") or "",
                            "channel_title": snippet.get("channelTitle") or "",
                        }
            except Exception as exc:
                logger.warning("backfill videos.list batch %d error: %s", i // 50, exc)
                counts["api_errors"] += 1
            if i + 50 < len(vid_ids):
                await asyncio.sleep(1.0)

        # Collect unique channel_ids for channels.list
        channel_ids_seen: set[str] = set()
        channel_ids: list[str] = []
        for v in vid_to_channel.values():
            cid = v.get("channel_id") or ""
            if cid and cid not in channel_ids_seen:
                channel_ids_seen.add(cid)
                channel_ids.append(cid)

        channel_info = await _fetch_channel_info(client, channel_ids)

    # Write updates to DB
    with get_connection(db_path) as conn:
        for vid_id, ch in vid_to_channel.items():
            src_id = url_to_id.get(vid_id)
            if not src_id:
                continue
            cid = ch.get("channel_id") or None
            ctitle = ch.get("channel_title") or None
            info = channel_info.get(cid or "") if cid else {}
            country = (info or {}).get("country") or None
            default_lang = (info or {}).get("defaultLanguage") or None
            conn.execute(
                """UPDATE sources SET channel_id=?, channel_title=?,
                   channel_country=?, channel_default_language=? WHERE id=?""",
                (cid, ctitle, country, default_lang, src_id),
            )
            counts["updated"] += 1
        conn.commit()

    logger.info("backfill_channel_metadata: %s", counts)
    return counts
