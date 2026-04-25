from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
DATA_DB = ROOT / "data" / "processed" / "siamquantum_atlas.db"
TMP_DB = Path("/tmp") / "siamquantum_atlas.db"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _prepare_demo_db() -> str:
    """Copy bundled DB to /tmp (writable) so SQLite can use WAL mode."""
    if DATA_DB.exists():
        try:
            if (not TMP_DB.exists()) or DATA_DB.stat().st_mtime > TMP_DB.stat().st_mtime:
                shutil.copy2(DATA_DB, TMP_DB)
            return f"sqlite:///{TMP_DB.as_posix()}"
        except Exception as exc:
            # /tmp unavailable — fall back to read-only source path
            os.environ["SIAMQUANTUM_DATABASE_READ_ONLY"] = "true"
            return f"sqlite:///{DATA_DB.as_posix()}"
    return f"sqlite:///{(ROOT / 'data' / 'processed' / 'siamquantum_atlas.db').as_posix()}"


# Force-set — Vercel dashboard env vars must NOT override these
os.environ["SIAMQUANTUM_DEPLOYMENT_MODE"] = "vercel"
os.environ["SIAMQUANTUM_DATABASE_URL"] = _prepare_demo_db()

from siamquantum.viewer.server import app  # noqa: E402
