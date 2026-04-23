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
filter_app = typer.Typer(help="Content quality filter commands")

app.add_typer(db_app, name="db")
app.add_typer(ingest_app, name="ingest")
app.add_typer(analyze_app, name="analyze")
app.add_typer(filter_app, name="filter")


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


@db_app.command("audit")
def db_audit(
    fix: bool = typer.Option(False, "--fix", help="Apply deterministic integrity repairs"),
) -> None:
    """Run a compact DB integrity audit for orphans, stale abstentions, and duplicate links."""
    from siamquantum.pipeline.integrity import run_integrity_audit

    db_path = db_path_from_url(settings.database_url)
    report = run_integrity_audit(db_path, fix=fix)
    typer.echo(
        f"geo_isp_sync_candidates={report['geo_isp_sync_candidates']}"
        f" stale_abstentions={report['stale_abstentions_with_triplets']}"
        f" duplicate_link_groups={report['duplicate_graph_links']['groups']}"
        f" duplicate_link_rows={report['duplicate_graph_links']['extra_rows']}"
    )
    typer.echo(
        f"orphans entities={report['orphans']['entities']}"
        f" triplets={report['orphans']['triplets']}"
        f" abstentions={report['orphans']['abstentions']}"
    )
    if fix:
        typer.echo(
            f"fixed geo_isp_synced={report['fixed']['geo_isp_synced']}"
            f" stale_abstentions_removed={report['fixed']['stale_abstentions_removed']}"
            f" duplicate_graph_links_removed={report['fixed']['duplicate_graph_links_removed']}"
        )


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


@ingest_app.command("rss")
def ingest_rss(
    feed: str = typer.Option("all", "--feed", help="Feed name: narit|tint|sciencefocus|all"),
) -> None:
    """Fetch RSS feeds from Thai science/quantum sites and write to DB."""
    from siamquantum.services.rss import fetch_rss, FEEDS
    from siamquantum.pipeline.ingest import _insert_sources  # type: ignore[attr-defined]

    db_path = db_path_from_url(settings.database_url)
    feed_names = list(FEEDS.keys()) if feed == "all" else [feed]

    for name in feed_names:
        typer.echo(f"Fetching RSS feed: {name}")
        result = fetch_rss(name)
        if not result.ok:
            typer.echo(f"  ERROR: {result.error}", err=True)
            raise typer.Exit(1)
        records = result.data or []
        inserted = _insert_sources(records, db_path)
        typer.echo(f"  fetched={len(records)}  inserted={inserted}")


@ingest_app.command("cse")
def ingest_cse(
    tier: str = typer.Option("all", "--tier", help="Tier: academic|media|all"),
    year: int | None = typer.Option(None, "--year", help="Single year"),
    all_years: bool = typer.Option(False, "--all-years", help="Ingest 2018 to current year"),
) -> None:
    """Fetch Google Custom Search Engine results for Thai quantum content."""
    from siamquantum.services.google_cse import fetch_cse_yearly, probe_or_query, QuotaExhaustedError
    from siamquantum.pipeline.ingest import _insert_sources  # type: ignore[attr-defined]
    import datetime as _dt

    if year is None and not all_years:
        typer.echo("Pass --year YYYY or --all-years", err=True)
        raise typer.Exit(1)

    db_path = db_path_from_url(settings.database_url)
    current_year = _dt.date.today().year
    years = list(range(2018, current_year + 1)) if all_years else [year]  # type: ignore[arg-type]
    tiers: list[str] = ["academic", "media"] if tier == "all" else [tier]

    typer.echo("Probing CSE OR-query support...")
    use_or = probe_or_query("academic")
    typer.echo(f"  OR-query: {'supported' if use_or else 'fallback to Thai-only'}")

    for yr in years:
        for t in tiers:
            typer.echo(f"Fetching CSE tier={t} year={yr}")
            try:
                result = fetch_cse_yearly(yr, t, use_or_query=use_or)  # type: ignore[arg-type]
            except QuotaExhaustedError as exc:
                typer.echo(f"  QUOTA EXHAUSTED: {exc}", err=True)
                raise typer.Exit(1)
            if not result.ok:
                typer.echo(f"  ERROR: {result.error}", err=True)
                raise typer.Exit(1)
            records = result.data or []
            inserted = _insert_sources(records, db_path)
            typer.echo(f"  fetched={len(records)}  inserted={inserted}")


@ingest_app.command("seeds")
def ingest_seeds() -> None:
    """Fetch hand-curated seed URLs and write to DB."""
    from siamquantum.services.seeds import fetch_seeds
    from siamquantum.pipeline.ingest import _insert_sources  # type: ignore[attr-defined]

    db_path = db_path_from_url(settings.database_url)
    typer.echo("Fetching seed URLs...")
    result = fetch_seeds()
    if not result.ok:
        typer.echo(f"  ERROR: {result.error}", err=True)
        raise typer.Exit(1)
    records = result.data or []
    inserted = _insert_sources(records, db_path)
    typer.echo(f"  fetched={len(records)}  inserted={inserted}")


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


@ingest_app.command("channel-backfill")
def ingest_channel_backfill() -> None:
    """Backfill channel_id/title/country/language for existing YouTube rows."""
    from siamquantum.pipeline.ingest import backfill_channel_metadata

    db_path = db_path_from_url(settings.database_url)
    typer.echo("Backfilling YouTube channel metadata (videos.list + channels.list)…")
    counts = asyncio.run(backfill_channel_metadata(db_path))
    typer.echo(
        f"  updated={counts['updated']}"
        f"  skipped_no_video={counts['skipped_no_video']}"
        f"  api_errors={counts['api_errors']}"
    )


# ---------------------------------------------------------------------------
# analyze commands
# ---------------------------------------------------------------------------

@analyze_app.command("nlp")
def analyze_nlp(
    year: int = typer.Option(0, "--year", help="4-digit year (or use --all)"),
    all_years: bool = typer.Option(False, "--all", help="Process all years present in DB"),
) -> None:
    """Run NLP pipeline (triplet extraction + entity classification) for a year or all years."""
    from siamquantum.pipeline.nlp import analyze_year

    db_path = db_path_from_url(settings.database_url)

    if all_years:
        from siamquantum.db.session import get_connection
        with get_connection(db_path) as conn:
            years = [
                int(row[0])
                for row in conn.execute(
                    "SELECT DISTINCT published_year FROM sources ORDER BY published_year"
                ).fetchall()
                if row[0] is not None
            ]
    elif year:
        years = [year]
    else:
        typer.echo("Pass --year YYYY or --all", err=True)
        raise typer.Exit(1)

    total: dict[str, int] = {
        "processed": 0, "skipped_already_done": 0, "skipped_no_text": 0,
        "discarded_duplicate": 0, "failed": 0,
        "triplets_written": 0, "entities_written": 0,
        "token_input": 0, "token_output": 0,
    }
    for yr in years:
        typer.echo(f"NLP year={yr}…")
        counts = analyze_year(yr, db_path)
        for k in total:
            total[k] += counts.get(k, 0)
        typer.echo(
            f"  processed={counts['processed']}"
            f"  skip={counts['skipped_already_done']}"
            f"  no_text={counts['skipped_no_text']}"
            f"  failed={counts.get('failed', 0)}"
            f"  triplets={counts.get('triplets_written', 0)}"
        )

    tok_in = total["token_input"]
    tok_out = total["token_output"]
    cost = tok_in * 3.0 / 1_000_000 + tok_out * 15.0 / 1_000_000
    typer.echo("=== TOTAL ===")
    typer.echo(
        f"  processed={total['processed']}"
        f"  skipped_already_done={total['skipped_already_done']}"
        f"  skipped_no_text={total['skipped_no_text']}"
        f"  discarded_duplicate={total['discarded_duplicate']}"
        f"  failed={total['failed']}"
    )
    typer.echo(
        f"  triplets_written={total['triplets_written']}"
        f"  entities_written={total['entities_written']}"
    )
    typer.echo(f"  tokens: input={tok_in} output={tok_out}  cost_usd=${cost:.4f}")


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
        f"  bootstrap_yearly={result.get('bootstrap_yearly_computed', '?')}"
        f"  bootstrap_pairwise={result.get('bootstrap_pairwise_computed', '?')}"
    )
    trend = result.get("bootstrap_trend") or {}
    typer.echo(
        f"  trend: MK_tau={trend.get('mannkendall_tau', '?')}"
        f"  MK_p={trend.get('mannkendall_p', '?')}"
        f"  Spearman_rho={trend.get('spearman_rho', '?')}"
        f"  Spearman_p={trend.get('spearman_p', '?')}"
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
# filter commands
# ---------------------------------------------------------------------------

@filter_app.command("relevance")
def filter_relevance(
    all_sources: bool = typer.Option(False, "--all", help="Run on all unchecked sources"),
) -> None:
    """Classify sources for quantum-tech relevance and Thailand relatedness."""
    if not all_sources:
        typer.echo("Pass --all to run on all unchecked sources.", err=True)
        raise typer.Exit(1)

    from siamquantum.db.session import get_connection
    db_path = db_path_from_url(settings.database_url)
    with get_connection(db_path) as conn:
        pending = conn.execute(
            "SELECT COUNT(*) FROM sources WHERE relevance_checked_at IS NULL"
        ).fetchone()[0]

    # Cost estimate: ~400 input + 80 output tokens per source (Sonnet pricing)
    est_input_tok = pending * 400
    est_output_tok = pending * 80
    est_cost = est_input_tok * 3.0 / 1_000_000 + est_output_tok * 15.0 / 1_000_000
    typer.echo(
        f"Pending sources: {pending}\n"
        f"Cost estimate: input={est_input_tok:,} tok  output={est_output_tok:,} tok"
        f"  ~${est_cost:.2f} USD"
    )
    if est_cost > 3.0:
        typer.echo(f"BUDGET EXCEEDED: ${est_cost:.2f} > $3.00. Aborting.", err=True)
        raise typer.Exit(1)

    from siamquantum.pipeline.filter import backfill_relevance
    typer.echo("Running relevance classifier...")
    counts = backfill_relevance(db_path)

    actual_cost = counts.get("cost_usd_cents", 0) / 100
    typer.echo(
        f"\nDone.\n"
        f"  checked:              {counts['checked']}\n"
        f"  accepted (Q+TH):      {counts['accepted']}\n"
        f"  rejected not quantum: {counts['rejected_not_quantum']}\n"
        f"  rejected not thai:    {counts['rejected_not_thai']}\n"
        f"  rejected both:        {counts['rejected_both']}\n"
        f"  failed:               {counts['failed']}\n"
        f"  tokens in/out: {counts.get('token_input', 0):,} / {counts.get('token_output', 0):,}\n"
        f"  actual cost: ${actual_cost:.4f} USD"
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
