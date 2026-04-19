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


@ingest_app.command("asn-backfill")
def ingest_asn_backfill() -> None:
    """Populate asn_org / is_cdn_resolved for existing geo rows (requires GeoLite2-ASN.mmdb)."""
    from siamquantum.pipeline.ingest import backfill_asn

    db_path = db_path_from_url(settings.database_url)
    typer.echo("Running ASN backfill for existing geo rows…")
    counts = backfill_asn(db_path)
    typer.echo(
        f"  updated={counts['updated']}"
        f"  skipped_no_ip={counts['skipped_no_ip']}"
        f"  skipped_no_asn_db={counts['skipped_no_asn_db']}"
    )


# ---------------------------------------------------------------------------
# analyze commands
# ---------------------------------------------------------------------------

@analyze_app.command("nlp")
def analyze_nlp(year: int = typer.Option(..., "--year")) -> None:
    """Run NLP pipeline (triplet extraction + entity classification) for a year."""
    from siamquantum.pipeline.nlp import analyze_year

    db_path = db_path_from_url(settings.database_url)
    typer.echo(f"Running NLP pipeline for year={year}…")
    counts = analyze_year(year, db_path)
    tok_in = counts.get("token_input", 0)
    tok_out = counts.get("token_output", 0)
    cost = tok_in * 3.0 / 1_000_000 + tok_out * 15.0 / 1_000_000
    typer.echo(
        f"  processed={counts['processed']}"
        f"  skipped_already_done={counts['skipped_already_done']}"
        f"  skipped_no_text={counts['skipped_no_text']}"
        f"  discarded_duplicate={counts['discarded_duplicate']}"
    )
    typer.echo(
        f"  triplets_written={counts.get('triplets_written', '?')}"
        f"  entities_written={counts.get('entities_written', '?')}"
    )
    typer.echo(
        f"  tokens: input={tok_in} output={tok_out}"
        f"  cost_usd=${cost:.4f}"
    )


@analyze_app.command("stats")
def analyze_stats() -> None:
    """Run DenStream clustering + Welch t-tests + update engagement levels."""
    from siamquantum.pipeline.analyze import run_stats

    db_path = db_path_from_url(settings.database_url)
    typer.echo("Running stats pipeline…")
    result = run_stats(db_path)
    typer.echo(
        f"  sources_processed={result['sources_processed']}"
        f"  engagement_levels_updated={result['engagement_levels_updated']}"
    )
    typer.echo(
        f"  micro_clusters={result['micro_clusters']}"
        f"  macro_clusters={result['macro_clusters']}"
    )
    typer.echo(
        f"  ttest_pairs_computed={result['ttest_pairs_computed']}"
        f"  ttest_pairs_skipped={result['ttest_pairs_skipped']}"
    )


@analyze_app.command("full")
def analyze_full() -> None:
    """Run NLP for years present in the DB, then run stats."""
    from siamquantum.pipeline.nlp import analyze_year
    from siamquantum.pipeline.analyze import run_stats
    from siamquantum.db.session import get_connection

    db_path = db_path_from_url(settings.database_url)
    with get_connection(db_path) as conn:
        years = [
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT published_year FROM sources ORDER BY published_year"
            ).fetchall()
            if row[0] is not None
        ]

    if not years:
        typer.echo("No source rows found. Run ingest first.", err=True)
        raise typer.Exit(1)

    for year in years:
        typer.echo(f"NLP year={year}...")
        counts = analyze_year(year, db_path)
        typer.echo(
            f"  processed={counts['processed']}"
            f" skipped={counts['skipped_already_done']}"
            f" no_text={counts['skipped_no_text']}"
        )
    typer.echo("Running stats...")
    result = run_stats(db_path)
    typer.echo(
        f"  macro_clusters={result['macro_clusters']}"
        f" ttest_pairs={result['ttest_pairs_computed']}"
    )


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------

@app.command("serve")
def serve(
    port: int = typer.Option(settings.viewer_port, "--port"),
    reload: bool = typer.Option(False, "--reload", help="Enable auto-reload for local development"),
) -> None:
    """Start the FastAPI viewer on port 8765."""
    import uvicorn

    typer.echo(f"Starting SiamQuantum Atlas viewer on http://localhost:{port}")
    # uvicorn.run is the blocking entrypoint for the CLI serve command.
    uvicorn.run(
        "siamquantum.viewer.server:app",
        host="0.0.0.0",
        port=port,
        reload=reload,
        log_level="info",
    )
