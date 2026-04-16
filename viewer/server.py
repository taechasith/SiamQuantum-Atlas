#!/usr/bin/env python3
"""
SiamQuantum Atlas — local viewer server with Claude API proxy.

Serves viewer/ as static files and exposes POST /api/analyze,
which calls Claude using the structured prompt in claudeprompt.json.

Usage:
    python viewer/server.py
    # or: make serve-viewer
"""
from __future__ import annotations

import json
import logging
import os
import pathlib
import random
import re
import sys
import threading
import time

from http.server import BaseHTTPRequestHandler, HTTPServer

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT         = pathlib.Path(__file__).parent          # viewer/
PROJECT_ROOT = ROOT.parent                            # SiamQuantum-Atlas/

# Load .env manually (avoid dependency on pydantic-settings at startup)
env_path = PROJECT_ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip())

ANTHROPIC_KEY = os.environ.get("SIAMQUANTUM_ANTHROPIC_API_KEY", "")
CLAUDE_MODEL  = os.environ.get("SIAMQUANTUM_CLAUDE_MODEL", "claude-sonnet-4-6")
PORT          = int(os.environ.get("SIAMQUANTUM_VIEWER_PORT", "8765"))

# ── Load claudeprompt.json ────────────────────────────────────────────────────
PROMPT_PATH = PROJECT_ROOT / "claudeprompt.json"
if not PROMPT_PATH.exists():
    print(f"[error] claudeprompt.json not found at {PROMPT_PATH}")
    sys.exit(1)

with open(PROMPT_PATH, encoding="utf-8") as f:
    CLAUDE_PROMPT = json.load(f)

SYSTEM_PROMPT = CLAUDE_PROMPT["system"]

# ── Anthropic client ──────────────────────────────────────────────────────────
try:
    from anthropic import Anthropic
    _client = Anthropic(api_key=ANTHROPIC_KEY) if ANTHROPIC_KEY else None
except ImportError:
    _client = None
    print("[warn] anthropic package not found — install with: pip install anthropic")

# ── MIME types ────────────────────────────────────────────────────────────────
MIME = {
    ".html": "text/html; charset=utf-8",
    ".js":   "text/javascript",
    ".json": "application/json",
    ".css":  "text/css",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".svg":  "image/svg+xml",
    ".ico":  "image/x-icon",
}

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("atlas")

OVERLOAD_STATUS_CODES = {429, 529}
OVERLOAD_ERROR_TYPES = {"overloaded_error", "rate_limit_error"}
REQUEST_ID_RE = re.compile(r'"request_id"\s*:\s*"([^"]+)"')

# ── Fetcher integration ───────────────────────────────────────────────────────
try:
    from build_realtime_graph import fetch_and_build, OUT as GRAPH_OUT
    FETCHER_AVAILABLE = True
except ImportError:
    FETCHER_AVAILABLE = False
    GRAPH_OUT = ROOT / "data" / "siamquantum_atlas_graph.json"

fetch_state: dict = {"status": "idle", "last_fetched": None, "node_count": 0, "edge_count": 0, "error": None}
REFRESH_INTERVAL = 30 * 60  # 30 minutes
AUTO_REFRESH = os.environ.get("SIAMQUANTUM_VIEWER_AUTO_REFRESH", "").strip().lower() in {"1", "true", "yes", "on"}


def _graph_counts(graph: dict) -> tuple[int, int]:
    return len(graph.get("nodes", [])), len(graph.get("links", []))


def _load_existing_graph() -> dict | None:
    if not GRAPH_OUT.exists():
        return None
    try:
        with open(GRAPH_OUT, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("  Existing graph could not be read: %s", e)
        return None


def _sync_fetch_state_from_disk() -> None:
    graph = _load_existing_graph()
    if not graph:
        return
    node_count, edge_count = _graph_counts(graph)
    fetch_state.update({
        "status": "ready" if node_count or edge_count else "idle",
        "last_fetched": (
            graph.get("metadata", {}).get("fetched_at")
            or graph.get("meta", {}).get("fetched_at")
        ),
        "node_count": node_count,
        "edge_count": edge_count,
        "error": None,
    })

def _do_fetch():
    fetch_state["status"] = "fetching"
    fetch_state["error"] = None
    log.info("  Building realtime viewer graph...")
    try:
        graph = fetch_and_build(log_progress=True)
        node_count, edge_count = _graph_counts(graph)
        fetch_state.update({
            "status": "ready",
            "last_fetched": (
                graph.get("metadata", {}).get("fetched_at")
                or graph.get("meta", {}).get("fetched_at")
                or __import__("datetime").datetime.utcnow().isoformat() + "Z"
            ),
            "node_count": node_count,
            "edge_count": edge_count,
        })
        log.info("  Data ready: %d nodes / %d edges", fetch_state["node_count"], fetch_state["edge_count"])
    except Exception as e:
        fetch_state["status"] = "error"
        fetch_state["error"] = str(e)
        log.error("  Fetch failed: %s", e)

def _fetch_loop():
    while True:
        _do_fetch()
        time.sleep(REFRESH_INTERVAL)

def start_background_fetch():
    if not FETCHER_AVAILABLE:
        log.warning("  Realtime graph builder not available; serving existing graph data")
        return
    existing_graph = _load_existing_graph()
    if existing_graph is not None:
        node_count, edge_count = _graph_counts(existing_graph)
        fetch_state.update({
            "status": "ready" if node_count or edge_count else "idle",
            "last_fetched": (
                existing_graph.get("metadata", {}).get("fetched_at")
                or existing_graph.get("meta", {}).get("fetched_at")
            ),
            "node_count": node_count,
            "edge_count": edge_count,
            "error": None,
        })
        if node_count or edge_count:
            log.info("  Using existing graph: %d nodes / %d edges", node_count, edge_count)
            return
        log.warning("  Existing graph is empty; startup auto-refresh is disabled to avoid overwriting viewer data")
        return
    if not AUTO_REFRESH:
        log.info("  No viewer graph found. Auto-refresh is disabled; run build_realtime_graph.py manually to create one")
        return
    t = threading.Thread(target=_fetch_loop, daemon=True)
    t.start()
    log.info("  Background realtime builder started (refresh every 30 min)")


def _extract_request_id(message: str) -> str | None:
    match = REQUEST_ID_RE.search(message)
    return match.group(1) if match else None


def _extract_provider_error_payload(message: str) -> dict:
    start = message.find("{")
    if start == -1:
        return {}
    try:
        return json.loads(message[start:])
    except json.JSONDecodeError:
        return {}


def _is_transient_overload_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code in OVERLOAD_STATUS_CODES:
        return True

    payload = _extract_provider_error_payload(str(exc))
    error_type = (payload.get("error") or {}).get("type")
    if error_type in OVERLOAD_ERROR_TYPES:
        return True

    message = str(exc).lower()
    return "overloaded" in message or "rate limit" in message


def _format_api_error(exc: Exception) -> dict:
    message = str(exc)
    request_id = _extract_request_id(message)
    payload = _extract_provider_error_payload(message)
    error_type = (payload.get("error") or {}).get("type")
    status_code = getattr(exc, "status_code", None)

    if _is_transient_overload_error(exc):
        user_message = "The AI provider is overloaded right now. Please retry in a few seconds."
    else:
        user_message = message

    result = {"error": user_message}
    if request_id:
        result["request_id"] = request_id
    if status_code:
        result["status_code"] = status_code
    if error_type:
        result["error_type"] = error_type
    return result


def _create_anthropic_analysis(user_message: str):
    return _client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1200,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )


def _request_analysis_with_retry(user_message: str, *, max_attempts: int = 6):
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return _create_anthropic_analysis(user_message)
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts or not _is_transient_overload_error(exc):
                raise
            # Exponential backoff: 2s, 4s, 8s, 16s, 30s (capped) + jitter
            delay = min(30.0, 2.0 * (2 ** (attempt - 1))) + random.uniform(0, 2.0)
            request_id = _extract_request_id(str(exc))
            log.warning(
                "  ↳ Claude overloaded (attempt %d/%d, retrying in %.1fs)%s",
                attempt,
                max_attempts,
                delay,
                f" request_id={request_id}" if request_id else "",
            )
            time.sleep(delay)
    raise last_exc or RuntimeError("Analysis request failed")


# ── Request handler ───────────────────────────────────────────────────────────
class AtlasHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # suppress default access log
        pass

    # ── GET: static file server ───────────────────────────────────────────────
    def do_GET(self):
        path = self.path.split("?")[0].split("#")[0]
        if path == "/api/status":
            self._handle_status()
            return

        if path in ("", "/"):
            path = "/index.html"

        file_path = ROOT / path.lstrip("/")

        # security: stay inside ROOT
        try:
            file_path.resolve().relative_to(ROOT.resolve())
        except ValueError:
            self.send_error(403)
            return

        if not file_path.exists() or not file_path.is_file():
            self.send_error(404, f"Not found: {path}")
            return

        mime = MIME.get(file_path.suffix.lower(), "application/octet-stream")
        data = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self._cors()
        self.end_headers()
        self.wfile.write(data)

    # ── GET: /api/status ─────────────────────────────────────────────────────
    def _handle_status(self):
        self._json({**fetch_state, "refresh_interval_sec": REFRESH_INTERVAL})

    # ── POST: Claude proxy ────────────────────────────────────────────────────
    def do_POST(self):
        if self.path != "/api/analyze":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length) or b"{}")
        except Exception as e:
            self._json({"error": f"Bad request: {e}"}, 400)
            return

        result = analyze_node(body)
        self._json(result)

    # ── OPTIONS: CORS preflight ───────────────────────────────────────────────
    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

    def _json(self, obj: dict, status: int = 200):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self._cors()
        self.end_headers()
        self.wfile.write(data)


# ── Claude analysis ───────────────────────────────────────────────────────────
def analyze_node(node: dict) -> dict:
    """
    Call Claude using claudeprompt.json system prompt.
    Input: a graph node dict with fields: name/id, description, platform, layer, etc.
    Returns: structured JSON matching claudeprompt.json output_schema.
    """
    if _client is None:
        return {"error": "Anthropic client not available — check API key and package install"}

    # Map graph node fields → claudeprompt.json input schema
    user_payload = {
        "title":           node.get("name") or node.get("id") or "",
        "description":     node.get("description") or node.get("desc") or node.get("summary") or "",
        "content":         node.get("content") or node.get("body") or node.get("transcript") or "",
        "platform":        node.get("platform") or "",
        "media_type_hint": (node.get("layer") or "").replace("_", " ").lower(),
        "language":        node.get("language") or "mixed",
    }

    user_message = json.dumps(user_payload, ensure_ascii=False)

    try:
        log.info("  ↳ Claude: analyzing '%s' (%s)", node.get("name", node.get("id", "?")), node.get("layer", ""))
        response = _request_analysis_with_retry(user_message)
        text = response.content[0].text.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            text = text.rsplit("```", 1)[0].strip()
        result = json.loads(text)
        result["_cached"] = False
        result["_model"]  = CLAUDE_MODEL
        return result
    except json.JSONDecodeError as e:
        return {"error": f"Claude returned non-JSON: {e}", "_raw": text[:500]}
    except Exception as e:
        formatted = _format_api_error(e)
        log.error(
            "  ↳ Claude analysis failed%s: %s",
            f" request_id={formatted.get('request_id')}" if formatted.get("request_id") else "",
            str(e),
        )
        return formatted


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _sync_fetch_state_from_disk()
    server = HTTPServer(("localhost", PORT), AtlasHandler)

    print()
    print("  SiamQuantum Atlas Viewer")
    print("  -------------------------")
    print(f"  http://localhost:{PORT}")
    print(f"  Model  : {CLAUDE_MODEL}")
    print(f"  API key: {'OK' if ANTHROPIC_KEY else 'MISSING - set SIAMQUANTUM_ANTHROPIC_API_KEY in .env'}")
    print(f"  Prompt : {PROMPT_PATH.name}")
    print()
    print("  Ctrl+C to stop\n")

    start_background_fetch()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
