from __future__ import annotations

import logging
import time
from pathlib import Path
from urllib.parse import urlparse

import geoip2.database
import geoip2.errors

from siamquantum.models import GeoResult

logger = logging.getLogger(__name__)

_CITY_MMDB_PATH = Path("data/geoip/GeoLite2-City.mmdb")
_ASN_MMDB_PATH = Path("data/geoip/GeoLite2-ASN.mmdb")

_city_reader: geoip2.database.Reader | None = None
_city_checked: bool = False
_asn_reader: geoip2.database.Reader | None = None
_asn_checked: bool = False

_IPAPI_MIN_INTERVAL: float = 1.4
_ipapi_last_call: float = 0.0

# Case-insensitive substrings matching known CDN / cloud-hosting ASN orgs.
_CDN_ORG_FRAGMENTS: tuple[str, ...] = (
    "cloudflare",
    "akamai",
    "fastly",
    "amazon",
    "aws",
    "google",
    "microsoft",
    "azure",
    "incapsula",
    "imperva",
    "sucuri",
    "keycdn",
    "bunnycdn",
    "cdn77",
    "datacamp",       # CDN77 parent
    "limelight",
    "stackpath",
    "edgecast",
    "verizon digital",
    "zscaler",
    "digitalocean",
    "linode",
    "ovh",
)


def _get_city_reader() -> geoip2.database.Reader | None:
    global _city_reader, _city_checked
    if _city_checked:
        return _city_reader
    _city_checked = True
    if not _CITY_MMDB_PATH.exists():
        logger.warning("GeoLite2-City.mmdb not found at %s", _CITY_MMDB_PATH)
        return None
    try:
        _city_reader = geoip2.database.Reader(str(_CITY_MMDB_PATH))
        logger.info("MaxMind City reader opened: %s", _CITY_MMDB_PATH)
    except Exception as exc:
        logger.warning("Failed to open MaxMind City reader: %s", exc)
    return _city_reader


def _get_asn_reader() -> geoip2.database.Reader | None:
    global _asn_reader, _asn_checked
    if _asn_checked:
        return _asn_reader
    _asn_checked = True
    if not _ASN_MMDB_PATH.exists():
        logger.warning("GeoLite2-ASN.mmdb not found at %s — run scripts/download_geoip.sh", _ASN_MMDB_PATH)
        return None
    try:
        _asn_reader = geoip2.database.Reader(str(_ASN_MMDB_PATH))
        logger.info("MaxMind ASN reader opened: %s", _ASN_MMDB_PATH)
    except Exception as exc:
        logger.warning("Failed to open MaxMind ASN reader: %s", exc)
    return _asn_reader


def lookup_asn(ip: str) -> tuple[str | None, bool | None]:
    """
    Look up ASN organisation for an IP.
    Returns (asn_org, is_cdn_resolved).
    Returns (None, None) if ASN DB unavailable or IP not found.
    """
    reader = _get_asn_reader()
    if not reader:
        return None, None
    try:
        record = reader.asn(ip)
        org = record.autonomous_system_organization or None
        if org is None:
            return None, None
        org_lower = org.lower()
        is_cdn = any(frag in org_lower for frag in _CDN_ORG_FRAGMENTS)
        return org, is_cdn
    except geoip2.errors.AddressNotFoundError:
        return None, None
    except Exception as exc:
        logger.debug("ASN lookup failed for %s: %s", ip, exc)
        return None, None


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
    ip-api.com fallback (free tier: 45 req/min, HTTP only).
    Rate-limited to _IPAPI_MIN_INTERVAL seconds between calls.

    CDN/cloud IPs return datacenter coordinates — not content-origin location.
    ASN fields are populated separately via lookup_asn().
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
            logger.debug("ip-api non-success for %s: %s", ip, data.get("message"))
            return None

        lat = data.get("lat")
        lon = data.get("lon")
        if lat is None or lon is None:
            return None

        asn_org, is_cdn = lookup_asn(ip)
        return GeoResult(
            ip=ip,
            lat=float(lat),
            lng=float(lon),
            city=data.get("city") or None,
            region=data.get("regionName") or None,
            isp=data.get("isp") or None,
            asn_org=asn_org,
            is_cdn_resolved=is_cdn,
        )
    except Exception as exc:
        logger.warning("ip-api lookup failed for %s: %s", ip, exc)
        return None


def lookup(url: str) -> GeoResult | None:
    """
    Resolve URL domain → IP → MaxMind City, with ip-api.com fallback.
    ASN org and CDN flag always populated when GeoLite2-ASN.mmdb is present.

    CDN-hosted domains return datacenter coordinates — is_cdn_resolved=True
    signals this on the result so the viewer can filter or dim those points.
    """
    try:
        ip = resolve_domain(url)
        if not ip:
            return None

        asn_org, is_cdn = lookup_asn(ip)

        city_reader = _get_city_reader()
        if city_reader:
            try:
                record = city_reader.city(ip)
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
                        isp=None,  # GeoLite2-City has no ISP; isp comes from ip-api only
                        asn_org=asn_org,
                        is_cdn_resolved=is_cdn,
                    )
                logger.debug("MaxMind City: no coordinates for %s — trying ip-api", ip)
            except geoip2.errors.AddressNotFoundError:
                logger.debug("MaxMind City: IP not in DB for %s — trying ip-api", ip)

        result = _ipapi_lookup(ip)
        if result and asn_org is not None:
            # Overwrite ASN fields from MaxMind (more accurate than ip-api ISP string)
            return result.model_copy(update={"asn_org": asn_org, "is_cdn_resolved": is_cdn})
        return result

    except Exception as exc:
        logger.warning("GeoIP lookup failed for %s: %s", url, exc)
        return None
