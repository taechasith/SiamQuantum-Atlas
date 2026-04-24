from __future__ import annotations

import os
import sys
from pathlib import Path

# Vercel runs from /var/task; resolve repo root from this file's location
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Set absolute DB path before Settings() is instantiated
_db_path = ROOT / "data" / "processed" / "siamquantum_atlas.db"
os.environ.setdefault("SIAMQUANTUM_DATABASE_URL", f"sqlite:///{_db_path}")

# Activate demo/read-only mode on Vercel
os.environ.setdefault("SIAMQUANTUM_DEPLOYMENT_MODE", "vercel_demo")
os.environ.setdefault("SIAMQUANTUM_DATABASE_READ_ONLY", "true")

from siamquantum.viewer.server import app  # noqa: E402

__all__ = ["app"]
