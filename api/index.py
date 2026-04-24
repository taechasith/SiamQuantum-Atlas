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
    if DATA_DB.exists():
        try:
            if (not TMP_DB.exists()) or DATA_DB.stat().st_mtime > TMP_DB.stat().st_mtime:
                shutil.copy2(DATA_DB, TMP_DB)
            return f"sqlite:///{TMP_DB.as_posix()}"
        except Exception:
            return f"sqlite:///{DATA_DB.as_posix()}"
    return "sqlite:///data/processed/siamquantum_atlas.db"


os.environ.setdefault("SIAMQUANTUM_DEPLOYMENT_MODE", "vercel")
os.environ.setdefault("SIAMQUANTUM_DATABASE_URL", _prepare_demo_db())

from siamquantum.viewer.server import app
