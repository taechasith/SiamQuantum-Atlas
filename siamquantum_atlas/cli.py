from __future__ import annotations

import shutil

import typer
from dotenv import load_dotenv

from siamquantum_atlas.db.session import init_db
from siamquantum_atlas.ingestion.pipelines import PipelineRunner
from siamquantum_atlas.logging import configure_logging
from siamquantum_atlas.settings import settings
from siamquantum_atlas.utils.files import ensure_dir
from siamquantum_atlas.utils.viewer_tools import copy_export_to_viewer, open_viewer_in_browser, viewer_instructions

app = typer.Typer(add_completion=False, help="SiamQuantum-Atlas CLI")


def bootstrap() -> None:
    load_dotenv()
    configure_logging()
    ensure_dir(settings.raw_dir)
    ensure_dir(settings.processed_dir)
    ensure_dir(settings.exports_dir)
    init_db()


@app.command("setup")
def setup() -> None:
    bootstrap()
    env_path = settings.project_root / ".env"
    example_path = settings.project_root / ".env.example"
    if not env_path.exists() and example_path.exists():
        shutil.copy2(example_path, env_path)
    ensure_dir(settings.viewer_dir / "data")
    typer.echo("Setup complete.")
    typer.echo(viewer_instructions())


@app.command("backfill")
def backfill(years: int = typer.Option(10, "--years")) -> None:
    bootstrap()
    count = PipelineRunner().run_backfill(years)
    typer.echo(f"Backfill complete with {count} records.")


@app.command("refresh")
def refresh() -> None:
    bootstrap()
    count = PipelineRunner().run_refresh()
    typer.echo(f"Refresh complete with {count} records processed.")


@app.command("demo")
def demo() -> None:
    bootstrap()
    count = PipelineRunner().run_demo()
    typer.echo(f"Demo pipeline complete with {count} sample items.")


@app.command("export-arena")
def export_arena() -> None:
    bootstrap()
    output_path = PipelineRunner().export_latest_graph()
    target_path = copy_export_to_viewer(output_path)
    typer.echo(f"Graph export written to {output_path}")
    typer.echo(f"Copied to local viewer data at {target_path}")
    typer.echo(viewer_instructions())


@app.command("prepare-viewer")
def prepare_viewer() -> None:
    bootstrap()
    export_path = settings.exports_dir / "siamquantum_atlas_graph.json"
    if export_path.exists():
        typer.echo(f"Copied latest export to {copy_export_to_viewer(export_path)}")
    else:
        ensure_dir(settings.viewer_dir / "data")
    typer.echo(viewer_instructions())


@app.command("open-viewer")
def open_viewer() -> None:
    bootstrap()
    export_path = settings.exports_dir / "siamquantum_atlas_graph.json"
    if not export_path.exists():
        export_path = PipelineRunner().export_latest_graph()
    copy_export_to_viewer(export_path)
    url = open_viewer_in_browser()
    typer.echo(f"Viewer opened at {url}")


@app.command("prepare-arena", hidden=True)
def prepare_arena_compat() -> None:
    prepare_viewer()


@app.command("report")
def report() -> None:
    bootstrap()
    md_path, csv_path = PipelineRunner().generate_reports()
    typer.echo(f"Markdown report: {md_path}")
    typer.echo(f"CSV report: {csv_path}")


@app.command("dashboard")
def dashboard() -> None:
    bootstrap()
    typer.echo("Optional dashboard placeholder. Add a Streamlit UI if needed.")


@app.command("run")
def run() -> None:
    bootstrap()
    runner = PipelineRunner()
    count = runner.run_refresh()
    output_path = runner.export_latest_graph()
    typer.echo(f"Pipeline complete with {count} records. Latest graph export: {output_path}")


@app.command("realtime")
def realtime(max_items: int = typer.Option(1300, "--max-items", help="Item cap (default 1300)")) -> None:
    """Collect real-time Thailand quantum content and generate intelligence report."""
    bootstrap()
    from siamquantum_atlas.ingestion.realtime_pipeline import RealtimePipeline
    from siamquantum_atlas.reporting.intelligence_report import generate_intelligence_report
    from siamquantum_atlas.db.models import RealtimeRun
    from siamquantum_atlas.db.session import get_session

    typer.echo(f"Starting real-time collection (cap: {max_items} items)…")
    pipeline = RealtimePipeline()
    dataset = pipeline.run(max_items=max_items)

    typer.echo(f"Collected {dataset.total_items} items. Generating intelligence report…")
    output_dir = settings.exports_dir / "intelligence"
    paths = generate_intelligence_report(dataset, output_dir)

    # Persist run metadata
    with get_session() as session:
        run_record = RealtimeRun(
            total_items=dataset.total_items,
            platform_counts_json=dataset.platform_counts,
            cluster_counts_json=dataset.cluster_counts,
            report_paths_json={k: str(v) for k, v in paths.items()},
        )
        session.add(run_record)
        session.commit()

    typer.echo("\n=== Real-Time Intelligence Report ===")
    typer.echo(f"  Items collected:  {dataset.total_items}")
    typer.echo(f"  Platforms:        {dataset.platform_counts}")
    typer.echo(f"  Clusters:         {dataset.cluster_counts}")
    typer.echo(f"\n  JSON report:      {paths['json']}")
    typer.echo(f"  Markdown report:  {paths['markdown']}")
    typer.echo(f"  GEE GeoJSON:      {paths['geojson']}")
    typer.echo(f"  Items JSONL:      {paths['jsonl']}")


@app.command("intelligence-report")
def intelligence_report() -> None:
    """Re-generate intelligence report from last real-time collection (no new API calls)."""
    bootstrap()
    jsonl_path = settings.exports_dir / "intelligence" / "quantum_items.jsonl"
    if not jsonl_path.exists():
        typer.echo("No previous collection found. Run `realtime` first.", err=True)
        raise typer.Exit(1)

    import json
    from siamquantum_atlas.ingestion.realtime_pipeline import ProcessedItem, RealtimeDataset
    from siamquantum_atlas.reporting.intelligence_report import generate_intelligence_report

    items: list[ProcessedItem] = []
    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                items.append(ProcessedItem(**json.loads(line)))

    from collections import defaultdict, Counter
    platform_counts: dict = defaultdict(int)
    cluster_counts: dict = defaultdict(int)
    for item in items:
        platform_counts[item.platform] += 1
        cluster_counts[item.comm_value_cluster] += 1

    dataset = RealtimeDataset(
        collected_at=items[0].collected_at if items else "unknown",
        total_items=len(items),
        items=items,
        platform_counts=dict(platform_counts),
        cluster_counts=dict(cluster_counts),
    )

    output_dir = settings.exports_dir / "intelligence"
    paths = generate_intelligence_report(dataset, output_dir)
    typer.echo(f"Report regenerated from {len(items)} cached items.")
    typer.echo(f"  JSON:     {paths['json']}")
    typer.echo(f"  Markdown: {paths['markdown']}")
