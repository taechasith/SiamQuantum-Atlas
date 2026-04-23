"""Viewer API + page tests — aligned with current response envelope {ok,data,error}."""
from __future__ import annotations

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
        conn.execute(
            "INSERT INTO sources (platform,url,title,published_year,fetched_at,"
            "view_count,like_count,comment_count,is_quantum_tech,is_thailand_related,quantum_domain)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("gdelt", "https://bangkokpost.com/quantum", "Quantum in Thailand", 2024,
             now, None, None, None, 1, 1, "quantum_computing"),
        )
        conn.execute(
            "INSERT INTO sources (platform,url,title,published_year,fetched_at,"
            "view_count,like_count,comment_count,is_quantum_tech,is_thailand_related,quantum_domain)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("youtube", "https://youtube.com/watch?v=abc", "ควอนตัมคอมพิวเตอร์", 2024,
             now, 5000, 200, 30, 1, 1, "quantum_computing"),
        )
        conn.commit()
        # Geo for source 1
        conn.execute(
            "INSERT INTO geo (source_id,ip,lat,lng,city,region,isp,asn_org,is_cdn_resolved)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (1, "1.2.3.4", 13.75, 100.52, "Bangkok", "Bangkok",
             "TRUE INTERNET Co.,Ltd.", "TRUE INTERNET Co.,Ltd.", 0),
        )
        conn.execute(
            "INSERT INTO geo (source_id,ip,lat,lng,city,region,isp,asn_org,is_cdn_resolved)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (2, "104.18.0.1", 43.65, -79.38, "Toronto", None,
             "Cloudflare, Inc.", "Cloudflare, Inc.", 1),
        )
        conn.commit()
        # Entities (with taxonomy columns now included in schema via migration)
        conn.execute(
            "INSERT INTO entities (source_id,content_type,production_type,area,engagement_level,"
            "media_format,user_intent)"
            " VALUES (?,?,?,?,?,?,?)",
            (1, "news", "corporate_media", "quantum computing", "medium",
             "text_static", "information_news"),
        )
        conn.execute(
            "INSERT INTO entities (source_id,content_type,production_type,area,engagement_level,"
            "media_format,user_intent)"
            " VALUES (?,?,?,?,?,?,?)",
            (2, "educational", "independent", "quantum algorithms", "high",
             "video_long", "education_self_improvement"),
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
# Page routes render HTML with current body strings
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path,expected", [
    ("/dashboard",  "Thai Quantum Geo Dashboard"),
    ("/network",    "Quantum Relationship View"),
    ("/analytics",  "Bootstrap Pairwise Ratios"),
    ("/database",   "Source Database View"),
    ("/community",  "Community Submission"),
])
def test_pages_return_html(client: TestClient, path: str, expected: str) -> None:
    resp = client.get(path)
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert expected in resp.text, f"{path} body missing {expected!r}"


# ---------------------------------------------------------------------------
# GET /api/geo/list  — envelope: {ok, data, count, error}
# ---------------------------------------------------------------------------

def test_geo_list_schema(client: TestClient) -> None:
    resp = client.get("/api/geo/list")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert isinstance(payload["data"], list)
    assert isinstance(payload["count"], int)
    assert payload["relevance"]["mode"] == "operational_default"


def test_geo_list_default_excludes_cdn(client: TestClient) -> None:
    resp = client.get("/api/geo/list")
    payload = resp.json()
    rows = payload["data"]
    # Only origin IP (source 1, is_cdn_resolved=0) — CDN row excluded
    assert len(rows) == 1
    row = rows[0]
    required = {"source_id", "lat", "lng", "platform", "url", "published_year"}
    assert required.issubset(row.keys())
    assert row["lat"] == pytest.approx(13.75)
    assert row["is_cdn_resolved"] == 0


def test_geo_list_cdn_true_includes_all(client: TestClient) -> None:
    resp = client.get("/api/geo/list?cdn=true")
    assert resp.json()["count"] == 2


def test_geo_list_isp_populated(client: TestClient) -> None:
    resp = client.get("/api/geo/list?cdn=true")
    rows = resp.json()["data"]
    for row in rows:
        assert row.get("isp") is not None, f"isp missing on row {row.get('source_id')}"


# ---------------------------------------------------------------------------
# GET /api/graph  — envelope: {ok, data:{nodes,links}, error}
# ---------------------------------------------------------------------------

def test_graph_schema(client: TestClient) -> None:
    resp = client.get("/api/graph")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["relevance"]["mode"] == "operational_default"
    data = payload["data"]
    assert "nodes" in data and "links" in data
    assert len(data["nodes"]) >= 2
    node = data["nodes"][0]
    for field in ("id", "label", "val"):
        assert field in node, f"graph node missing field: {field}"


def test_graph_links_have_label(client: TestClient) -> None:
    resp = client.get("/api/graph")
    links = resp.json()["data"]["links"]
    assert len(links) >= 1
    link = links[0]
    for field in ("source", "target", "label"):
        assert field in link


# ---------------------------------------------------------------------------
# GET /api/graph/metrics  — envelope: {ok, data:{components,...}, error}
# ---------------------------------------------------------------------------

def test_graph_metrics_schema(client: TestClient) -> None:
    resp = client.get("/api/graph/metrics")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    data = payload["data"]
    assert "components" in data
    assert "largest_component_size" in data
    assert "top_degree" in data
    assert "top_betweenness" in data


# ---------------------------------------------------------------------------
# GET /api/stats/yearly  — envelope: {ok, data:{years,counts,...}, error}
# ---------------------------------------------------------------------------

def test_stats_yearly_schema(client: TestClient) -> None:
    resp = client.get("/api/stats/yearly")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    data = payload["data"]
    for key in ("years", "counts", "engagement_distribution", "pairwise", "trend"):
        assert key in data, f"stats/yearly missing key: {key}"
    assert "2024" in data["years"]
    counts_2024 = data["counts"]["2024"]
    assert counts_2024["total"] == 2
    assert counts_2024["gdelt"] == 1
    assert counts_2024["youtube"] == 1


def test_stats_yearly_method_field(client: TestClient) -> None:
    resp = client.get("/api/stats/yearly")
    payload = resp.json()
    data = payload["data"]
    assert data.get("method") == "bootstrap_geometric_mean"
    assert "operational" in data.get("relevance_scope_note", "")
    assert payload["relevance"]["mode"] == "operational_default"


# ---------------------------------------------------------------------------
# GET /api/sources (paginated)  — envelope: {ok, data:{total,items,...}, error}
# ---------------------------------------------------------------------------

def test_sources_pagination_schema(client: TestClient) -> None:
    resp = client.get("/api/sources")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["relevance"]["mode"] == "operational_default"
    data = payload["data"]
    assert "total" in data and "page" in data and "page_size" in data and "items" in data
    assert data["total"] == 2


def test_sources_filter_by_platform(client: TestClient) -> None:
    resp = client.get("/api/sources?platform=gdelt")
    data = resp.json()["data"]
    assert data["total"] == 1
    assert data["items"][0]["platform"] == "gdelt"


def test_sources_filter_by_content_type(client: TestClient) -> None:
    resp = client.get("/api/sources?content_type=news")
    assert resp.json()["data"]["total"] == 1


def test_sources_filter_by_media_format(client: TestClient) -> None:
    resp = client.get("/api/sources?media_format=video_long")
    assert resp.json()["data"]["total"] == 1


def test_sources_filter_by_user_intent(client: TestClient) -> None:
    resp = client.get("/api/sources?user_intent=information_news")
    assert resp.json()["data"]["total"] == 1


def test_sources_filter_by_quantum_domain(client: TestClient) -> None:
    resp = client.get("/api/sources?quantum_domain=quantum_computing")
    assert resp.json()["data"]["total"] == 2


def test_sources_items_have_taxonomy_fields(client: TestClient) -> None:
    resp = client.get("/api/sources")
    items = resp.json()["data"]["items"]
    assert len(items) == 2
    for item in items:
        assert "media_format" in item
        assert "user_intent" in item
        assert "quantum_domain" in item


# ---------------------------------------------------------------------------
# GET /api/taxonomy/summary
# ---------------------------------------------------------------------------

def test_taxonomy_summary_schema(client: TestClient) -> None:
    resp = client.get("/api/taxonomy/summary")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    data = payload["data"]
    assert "media_format" in data
    assert "user_intent" in data
    assert "thai_cultural_angle_count" in data
    assert "quantum_domain" in data
    assert len(data["media_format"]) >= 1


# ---------------------------------------------------------------------------
# GET /api/export/xlsx
# ---------------------------------------------------------------------------

def test_export_xlsx_returns_file(client: TestClient) -> None:
    resp = client.get("/api/export/xlsx")
    assert resp.status_code == 200
    ct = resp.headers["content-type"]
    assert "spreadsheetml" in ct or "xlsx" in ct
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert resp.content[:2] == b"PK"  # ZIP magic bytes


# ---------------------------------------------------------------------------
# POST /api/community/submit  — envelope: {ok, data:{id,status,...}, error}
# ---------------------------------------------------------------------------

def test_community_submit_ok(client: TestClient) -> None:
    resp = client.post("/api/community/submit", json={"url": "https://example.com/quantum"})
    assert resp.status_code == 201
    payload = resp.json()
    assert payload["ok"] is True
    data = payload["data"]
    assert "id" in data
    assert data["status"] == "queued"


def test_community_submit_missing_url(client: TestClient) -> None:
    resp = client.post("/api/community/submit", json={"handle": "user"})
    assert resp.status_code == 422
