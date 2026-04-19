"""Phase 6a gate tests — API endpoint schema validation."""
from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from siamquantum.db.session import init_db, get_connection
from siamquantum.viewer.server import app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def seeded_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "test.db"
    init_db(db_path)
    now = datetime.utcnow().isoformat()
    with get_connection(db_path) as conn:
        # Sources
        conn.execute(
            "INSERT INTO sources (platform,url,title,published_year,fetched_at,"
            "view_count,like_count,comment_count) VALUES (?,?,?,?,?,?,?,?)",
            ("gdelt", "https://bangkokpost.com/quantum", "Quantum in Thailand", 2024, now, None, None, None),
        )
        conn.execute(
            "INSERT INTO sources (platform,url,title,published_year,fetched_at,"
            "view_count,like_count,comment_count) VALUES (?,?,?,?,?,?,?,?)",
            ("youtube", "https://youtube.com/watch?v=abc", "ควอนตัมคอมพิวเตอร์", 2024, now, 5000, 200, 30),
        )
        conn.commit()
        # Geo for source 1
        conn.execute(
            "INSERT INTO geo (source_id,ip,lat,lng,city,region,isp,asn_org,is_cdn_resolved)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (1, "1.2.3.4", 13.75, 100.52, "Bangkok", "Bangkok", None, "TRUE INTERNET", 0),
        )
        conn.execute(
            "INSERT INTO geo (source_id,ip,lat,lng,city,region,isp,asn_org,is_cdn_resolved)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (2, "104.18.0.1", 43.65, -79.38, "Toronto", None, "Cloudflare", "Cloudflare, Inc.", 1),
        )
        conn.commit()
        # Entities
        conn.execute(
            "INSERT INTO entities (source_id,content_type,production_type,area,engagement_level)"
            " VALUES (?,?,?,?,?)",
            (1, "news", "corporate_media", "quantum computing", "medium"),
        )
        conn.execute(
            "INSERT INTO entities (source_id,content_type,production_type,area,engagement_level)"
            " VALUES (?,?,?,?,?)",
            (2, "educational", "independent", "quantum algorithms", "high"),
        )
        conn.commit()
        # Triplets
        conn.execute(
            "INSERT INTO triplets (source_id,subject,relation,object,confidence)"
            " VALUES (?,?,?,?,?)",
            (1, "Thailand", "develops", "quantum computing", 0.95),
        )
        conn.execute(
            "INSERT INTO triplets (source_id,subject,relation,object,confidence)"
            " VALUES (?,?,?,?,?)",
            (2, "quantum algorithms", "solve", "optimization problems", 0.88),
        )
        conn.commit()
    return db_path


@pytest.fixture()
def client(seeded_db: Path) -> TestClient:
    with patch("siamquantum.viewer.server.settings") as mock_settings:
        mock_settings.database_url = f"sqlite:///{seeded_db}"
        with TestClient(app) as c:
            yield c


# ---------------------------------------------------------------------------
# Root redirect
# ---------------------------------------------------------------------------

def test_root_redirects_to_dashboard(client: TestClient) -> None:
    resp = client.get("/", follow_redirects=False)
    assert resp.status_code in (301, 302, 307, 308)
    assert resp.headers["location"] == "/dashboard"


# ---------------------------------------------------------------------------
# Page routes render HTML
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path,expected", [
    ("/dashboard",  "Thai Quantum Source Map"),
    ("/network",    "คลิกโหนดเพื่อดูรายละเอียด"),
    ("/analytics",  "Welch"),
    ("/database",   "ฐานข้อมูล / Database"),
    ("/community",  "Submit a Source"),
])
def test_pages_return_html(client: TestClient, path: str, expected: str) -> None:
    resp = client.get(path)
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert expected in resp.text, f"{path} body missing {expected!r}"


# ---------------------------------------------------------------------------
# GET /api/geo/list
# ---------------------------------------------------------------------------

def test_geo_list_default_excludes_cdn(client: TestClient) -> None:
    resp = client.get("/api/geo/list")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    # Only origin IP (source 1, is_cdn_resolved=0) — CDN (source 2) excluded
    assert len(data) == 1
    row = data[0]
    required = {"source_id", "lat", "lng", "platform", "url", "published_year"}
    assert required.issubset(row.keys()), f"Missing keys: {required - row.keys()}"
    assert row["lat"] == pytest.approx(13.75)
    assert row["is_cdn_resolved"] == 0


def test_geo_list_cdn_true_includes_all(client: TestClient) -> None:
    resp = client.get("/api/geo/list?cdn=true")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2


def test_geo_list_has_asn_fields(client: TestClient) -> None:
    resp = client.get("/api/geo/list?cdn=true")
    data = resp.json()
    cdn_row = next(r for r in data if r["is_cdn_resolved"] == 1)
    assert cdn_row["asn_org"] == "Cloudflare, Inc."


# ---------------------------------------------------------------------------
# GET /api/graph
# ---------------------------------------------------------------------------

def test_graph_schema(client: TestClient) -> None:
    resp = client.get("/api/graph")
    assert resp.status_code == 200
    data = resp.json()
    assert "nodes" in data and "edges" in data
    assert len(data["nodes"]) == 2
    assert len(data["edges"]) == 2
    node = data["nodes"][0]
    for field in ("id", "content_type", "production_type", "area", "engagement_level", "url", "year", "platform"):
        assert field in node, f"Missing field: {field}"


# ---------------------------------------------------------------------------
# GET /api/stats/yearly
# ---------------------------------------------------------------------------

def test_stats_yearly_schema(client: TestClient) -> None:
    resp = client.get("/api/stats/yearly")
    assert resp.status_code == 200
    data = resp.json()
    assert "years" in data
    assert "counts" in data
    assert "engagement_distribution" in data
    assert "ttest_results" in data
    assert "macro_clusters" in data
    assert "2024" in data["years"]
    counts_2024 = data["counts"]["2024"]
    assert counts_2024["total"] == 2
    assert counts_2024["gdelt"] == 1
    assert counts_2024["youtube"] == 1


# ---------------------------------------------------------------------------
# GET /api/sources (paginated)
# ---------------------------------------------------------------------------

def test_sources_pagination_schema(client: TestClient) -> None:
    resp = client.get("/api/sources")
    assert resp.status_code == 200
    data = resp.json()
    assert "total" in data and "page" in data and "page_size" in data and "items" in data
    assert data["total"] == 2


def test_sources_filter_by_platform(client: TestClient) -> None:
    resp = client.get("/api/sources?platform=gdelt")
    data = resp.json()
    assert data["total"] == 1
    assert data["items"][0]["platform"] == "gdelt"


def test_sources_filter_by_content_type(client: TestClient) -> None:
    resp = client.get("/api/sources?content_type=news")
    data = resp.json()
    assert data["total"] == 1


# ---------------------------------------------------------------------------
# GET /api/export/xlsx
# ---------------------------------------------------------------------------

def test_export_xlsx_returns_file(client: TestClient) -> None:
    resp = client.get("/api/export/xlsx")
    assert resp.status_code == 200
    ct = resp.headers["content-type"]
    assert "spreadsheetml" in ct or "xlsx" in ct
    assert "attachment" in resp.headers.get("content-disposition", "")
    # Valid XLSX starts with PK (ZIP magic)
    assert resp.content[:2] == b"PK"


# ---------------------------------------------------------------------------
# POST /api/community/submit
# ---------------------------------------------------------------------------

def test_community_submit_ok(client: TestClient) -> None:
    resp = client.post("/api/community/submit", json={"url": "https://example.com/quantum"})
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["status"] == "pending"


def test_community_submit_missing_url(client: TestClient) -> None:
    resp = client.post("/api/community/submit", json={"handle": "user"})
    assert resp.status_code == 422
