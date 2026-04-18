from __future__ import annotations

"""Idempotent DB initialiser. Safe to run multiple times."""

import sys
from pathlib import Path

# Make sure src/ is on the path when run as a script
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from siamquantum.config import settings
from siamquantum.db.session import db_path_from_url, init_db


def main() -> None:
    db_path = db_path_from_url(settings.database_url)
    print(f"Initialising DB at: {db_path.resolve()}")
    init_db(db_path)
    print("Done. Tables created (or already existed).")


if __name__ == "__main__":
    main()
