from __future__ import annotations

import os
import socket
import subprocess
import sys
import webbrowser
from pathlib import Path

from siamquantum_atlas.settings import settings
from siamquantum_atlas.utils.files import ensure_dir


def viewer_data_path() -> Path:
    target = settings.viewer_dir / "data" / "siamquantum_atlas_graph.json"
    ensure_dir(target.parent)
    return target


def copy_export_to_viewer(export_path: Path) -> Path:
    target = viewer_data_path()
    target.write_bytes(export_path.read_bytes())
    return target


def viewer_url(port: int | None = None) -> str:
    resolved_port = port or settings.viewer_port
    return f"http://127.0.0.1:{resolved_port}/viewer/index.html"


def viewer_instructions(port: int | None = None) -> str:
    resolved_port = port or settings.viewer_port
    return (
        "Local viewer is ready.\n"
        f"Export JSON: {viewer_data_path()}\n"
        f"Open in browser: {viewer_url(resolved_port)}\n"
        f"Or run manually: python -m http.server {resolved_port}"
    )


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def start_viewer_server(port: int | None = None) -> str:
    resolved_port = port or settings.viewer_port
    ensure_dir(settings.viewer_dir / "data")
    if not _port_in_use(resolved_port):
        creationflags = 0
        if os.name == "nt":
            creationflags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        subprocess.Popen(
            [sys.executable, "-m", "http.server", str(resolved_port)],
            cwd=settings.project_root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    return viewer_url(resolved_port)


def open_viewer_in_browser(port: int | None = None) -> str:
    url = start_viewer_server(port)
    webbrowser.open(url)
    return url
