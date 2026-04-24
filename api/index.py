from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

# Vercel runs from /var/task; resolve repo root from this file's location
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Copy DB to /tmp (writable) so SQLite can create .db-shm/.db-wal temp files.
# /var/task is read-only on Vercel — any SQLite open attempt fails without this.
_src_db = ROOT / "data" / "processed" / "siamquantum_atlas.db"
_tmp_db = Path("/tmp/siamquantum_atlas.db")
if _src_db.exists() and not _tmp_db.exists():
    shutil.copy2(_src_db, _tmp_db)
_db_path = _tmp_db if _tmp_db.exists() else _src_db

os.environ.setdefault("SIAMQUANTUM_DATABASE_URL", f"sqlite:///{_db_path}")

# Activate demo/read-only mode on Vercel
os.environ.setdefault("SIAMQUANTUM_DEPLOYMENT_MODE", "vercel_demo")
os.environ.setdefault("SIAMQUANTUM_DATABASE_READ_ONLY", "true")

from siamquantum.viewer.server import app  # noqa: E402

__all__ = ["app"]
