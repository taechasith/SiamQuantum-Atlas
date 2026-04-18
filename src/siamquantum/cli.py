from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Windows ProactorEventLoop has SSL issues with httpx — use SelectorEventLoop
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import typer

from siamquantum.config import settings
from siamquantum.db.session import db_path_from_url, init_db

app = typer.Typer(help="SiamQuantum Atlas CLI")
db_app = typer.Typer(help="Database commands")
ingest_app = typer.Typer(help="Data ingestion commands")
analyze_app = typer.Typer(help="Analysis commands")

app.add_typer(db_app, name="db")
app.add_typer(ingest_app, name="ingest")
app.add_typer(analyze_app, name="analyze")


# ---------------------------------------------------------------------------
# db commands
# ---------------------------------------------------------------------------

@db_app.command("init")
def db_init() -> None:
    """Create database and run schema migrations (idempotent)."""
    db_path = db_path_from_url(settings.database_url)
    typer.echo(f"Initialising DB at: {db_path.resolve()}")
    init_db(db_path)
    typer.echo("Done.")


@db_app.command("reset")
def db_reset(
    confirm: bool = typer.Option(False, "--confirm", help="Required to proceed"),
) -> None:
    """Drop and recreate all tables. Destructive — requires --confirm."""
    if not confirm:
        typer.echo("Aborted. Pass --confirm to proceed.", err=True)
        raise typer.Exit(1)
    db_path = db_path_from_url(settings.database_url)
    if db_path.exists():
        db_path.unlink()
        typer.echo("Database deleted.")
    init_db(db_path)
    typer.echo("Database recreated.")


# ---------------------------------------------------------------------------
# ingest commands
# ---------------------------------------------------------------------------

@ingest_app.command("gdelt")
def ingest_gdelt(
    year: int = typer.Option(..., "--year", help="4-digit year to ingest"),
    all_years: bool = typer.Option(False, "--all-years", help="Ingest 2020 to last year"),
) -> None:
    """Fetch GDELT quantum articles for Thailand and write to DB."""
    from siamquantum.pipeline.ingest import ingest_gdelt_year
    from siamquantum.db.session import get_connection
    from siamquantum.db.repos import SourceRepo

    db_path = db_path_from_url(settings.database_url)
    years = list(range(2020, year + 1)) if all_years else [year]

    for yr in years:
        typer.echo(f"Fetching GDELT year={yr} …")
        try:
            fetched, inserted = asyncio.run(ingest_gdelt_year(yr, db_path))
        except RuntimeError as exc:
            typer.echo(f"  ERROR: {exc}", err=True)
            continue

        typer.echo(f"  fetched={fetched}  inserted={inserted}")

        # Print 3 sample records
        with get_connection(db_path) as conn:
            repo = SourceRepo(conn)
            samples = repo.list_by_year(yr)[:3]

        typer.echo(f"  Sample records (up to 3):")
        for s in samples:
            typer.echo(f"    [{s.id}] {s.platform} | {s.published_year} | {s.url[:80]}")


@ingest_app.command("youtube")
def ingest_youtube(
    year: int = typer.Option(..., "--year"),
    all_years: bool = typer.Option(False, "--all-years"),
) -> None:
    """Fetch YouTube quantum videos for Thailand."""
    from siamquantum.pipeline.ingest import ingest_youtube_year
    from siamquantum.db.session import get_connection
    from siamquantum.db.repos import SourceRepo

    db_path = db_path_from_url(settings.database_url)
    years = list(range(2020, year + 1)) if all_years else [year]

    for yr in years:
        typer.echo(f"Fetching YouTube year={yr} …")
        try:
            fetched, inserted = asyncio.run(ingest_youtube_year(yr, db_path))
        except RuntimeError as exc:
            typer.echo(f"  ERROR: {exc}", err=True)
            continue

        typer.echo(f"  fetched={fetched}  inserted={inserted}")

        with get_connection(db_path) as conn:
            repo = SourceRepo(conn)
            samples = [s for s in repo.list_by_year(yr) if s.platform == "youtube"][:3]

        typer.echo(f"  Sample records (up to 3):")
        for s in samples:
            typer.echo(f"    [{s.id}] {s.platform} | {s.published_year} | {s.url[:80]}")


@ingest_app.command("geo")
def ingest_geo(
    pending: bool = typer.Option(True, "--pending/--no-pending"),
) -> None:
    """Run GeoIP lookup for GDELT sources missing geo rows."""
    from siamquantum.pipeline.ingest import backfill_geo

    db_path = db_path_from_url(settings.database_url)
    typer.echo("Running GeoIP backfill for pending GDELT sources…")
    counts = backfill_geo(db_path)
    typer.echo(
        f"  success={counts['success']}"
        f"  failure={counts['failure']}"
        f"  skipped_youtube={counts['skipped_youtube']}"
    )


# ---------------------------------------------------------------------------
# analyze commands
# ---------------------------------------------------------------------------

@analyze_app.command("nlp")
def analyze_nlp(year: int = typer.Option(..., "--year")) -> None:
    """Run NLP pipeline for a year."""
    raise NotImplementedError("phase 4 not yet implemented — see SPEC.md")


@analyze_app.command("stats")
def analyze_stats() -> None:
    """Run yearly stats + t-tests."""
    raise NotImplementedError("phase 5 not yet implemented — see SPEC.md")


@analyze_app.command("full")
def analyze_full() -> None:
    """Run nlp + stats for all years."""
    raise NotImplementedError("phase 5 not yet implemented — see SPEC.md")


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------

@app.command("serve")
def serve(
    port: int = typer.Option(settings.viewer_port, "--port"),
    reload: bool = typer.Option(False, "--reload"),
) -> None:
    """Start the FastAPI viewer."""
    raise NotImplementedError("phase 6 not yet implemented — see SPEC.md")
