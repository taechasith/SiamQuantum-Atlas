from __future__ import annotations

import logging
import time
from pathlib import Path
from urllib.parse import urlparse

import geoip2.database
import geoip2.errors

from siamquantum.models import GeoResult

logger = logging.getLogger(__name__)

_MMDB_PATH = Path("data/geoip/GeoLite2-City.mmdb")

_reader_instance: geoip2.database.Reader | None = None
_reader_checked: bool = False

# ip-api rate limit: 45 req/min free tier → enforce 1.4s gap
_IPAPI_MIN_INTERVAL: float = 1.4
_ipapi_last_call: float = 0.0


def _reader() -> geoip2.database.Reader | None:
    global _reader_instance, _reader_checked
    if _reader_checked:
        return _reader_instance
    _reader_checked = True
    if not _MMDB_PATH.exists():
        logger.warning(
            "GeoLite2-City.mmdb not found at %s — run scripts/download_geoip.sh",
            _MMDB_PATH,
        )
        return None
    try:
        _reader_instance = geoip2.database.Reader(str(_MMDB_PATH))
        logger.info("MaxMind reader opened: %s", _MMDB_PATH)
    except Exception as exc:
        logger.warning("Failed to open MaxMind reader: %s", exc)
    return _reader_instance


def resolve_domain(url: str) -> str | None:
    """Extract hostname from URL and resolve to first A record IP."""
    try:
        import dns.resolver

        hostname = urlparse(url).hostname or ""
        if not hostname:
            logger.debug("No hostname in URL: %s", url)
            return None

        resolver = dns.resolver.Resolver()
        resolver.lifetime = 3.0
        answers = resolver.resolve(hostname, "A")
        return str(answers[0])
    except Exception as exc:
        logger.debug("DNS resolution failed for %s: %s", url, exc)
        return None


def _ipapi_lookup(ip: str) -> GeoResult | None:
    """
    ip-api.com fallback lookup (free tier: 45 req/min, HTTP only).
    Rate-limited to _IPAPI_MIN_INTERVAL seconds between calls.
    Returns None on any failure — never raises.

    Note: CDN/cloud IPs return the datacenter location, not content origin.
    This is a known limitation; result may not reflect Thai geography.
    """
    global _ipapi_last_call

    elapsed = time.monotonic() - _ipapi_last_call
    if elapsed < _IPAPI_MIN_INTERVAL:
        time.sleep(_IPAPI_MIN_INTERVAL - elapsed)

    try:
        import httpx

        _ipapi_last_call = time.monotonic()
        resp = httpx.get(
            f"http://ip-api.com/json/{ip}",
            params={"fields": "status,lat,lon,city,regionName,isp,country"},
            timeout=5.0,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "success":
            logger.debug("ip-api returned non-success for %s: %s", ip, data.get("message"))
            return None

        lat = data.get("lat")
        lon = data.get("lon")
        if lat is None or lon is None:
            return None

        return GeoResult(
            ip=ip,
            lat=float(lat),
            lng=float(lon),
            city=data.get("city") or None,
            region=data.get("regionName") or None,
            isp=data.get("isp") or None,
        )
    except Exception as exc:
        logger.warning("ip-api lookup failed for %s: %s", ip, exc)
        return None


def lookup(url: str) -> GeoResult | None:
    """
    Resolve URL domain → IP → MaxMind city lookup, with ip-api.com fallback.

    MaxMind GeoLite2-City has no ISP data; isp comes from ip-api fallback only.
    CDN-hosted domains (Fastly, Cloudflare, AWS) return datacenter coordinates,
    not the content-origin country — known limitation.

    Returns None on complete failure — never raises.
    """
    try:
        ip = resolve_domain(url)
        if not ip:
            return None

        reader = _reader()
        if reader:
            try:
                record = reader.city(ip)
                lat = record.location.latitude
                lng = record.location.longitude

                if lat is not None and lng is not None:
                    region: str | None = None
                    if record.subdivisions:
                        region = record.subdivisions.most_specific.name or None

                    return GeoResult(
                        ip=ip,
                        lat=float(lat),
                        lng=float(lng),
                        city=record.city.name or None,
                        region=region,
                        isp=None,  # GeoLite2-City has no ISP data
                    )
                logger.debug("MaxMind: no coordinates for IP %s — trying ip-api", ip)
            except geoip2.errors.AddressNotFoundError:
                logger.debug("MaxMind: IP not in DB for %s — trying ip-api", ip)
        else:
            logger.debug("MaxMind reader unavailable — trying ip-api for %s", ip)

        # Fallback: ip-api.com
        return _ipapi_lookup(ip)

    except Exception as exc:
        logger.warning("GeoIP lookup failed for %s: %s", url, exc)
        return None
