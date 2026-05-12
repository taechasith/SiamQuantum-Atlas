from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import typer

orch_app = typer.Typer(help="Prefect orchestration commands")

REPO_ROOT = Path(__file__).parent.parent.parent.parent


@orch_app.command("refresh")
def cmd_refresh() -> None:
    """Run the siamquantum-refresh flow once (blocking)."""
    from siamquantum.orchestration.prefect_flows import refresh_flow

    typer.echo("Running siamquantum-refresh flow…")
    result = refresh_flow()
    for step, data in result.items():
        typer.echo(f"  {step}: {data}")
    typer.echo("Done.")


@orch_app.command("healthcheck")
def cmd_healthcheck() -> None:
    """Run the siamquantum-healthcheck flow once (blocking)."""
    from siamquantum.orchestration.prefect_flows import healthcheck_flow

    typer.echo("Running siamquantum-healthcheck flow…")
    result = healthcheck_flow()
    typer.echo(f"  server:    {result['server']}")
    typer.echo(f"  freshness: {result['freshness']}")


@orch_app.command("serve")
def cmd_serve(
    refresh_cron: str = typer.Option(
        "0 1 * * *", "--refresh-cron", help="Cron for daily refresh (UTC)"
    ),
    health_cron: str = typer.Option(
        "0 * * * *", "--health-cron", help="Cron for hourly healthcheck (UTC)"
    ),
) -> None:
    """Start a local Prefect server and serve both flows on a cron schedule.

    No external Prefect server required — flows run in-process.
    Press Ctrl+C to stop.
    """
    from prefect import serve as prefect_serve

    from siamquantum.orchestration.prefect_flows import healthcheck_flow, refresh_flow

    typer.echo(f"Serving refresh flow on cron='{refresh_cron}'")
    typer.echo(f"Serving healthcheck flow on cron='{health_cron}'")
    typer.echo("Press Ctrl+C to stop.")

    refresh_dep = refresh_flow.to_deployment(
        name="siamquantum-refresh-scheduled",
        cron=refresh_cron,
        tags=["siamquantum", "refresh"],
    )
    health_dep = healthcheck_flow.to_deployment(
        name="siamquantum-healthcheck-scheduled",
        cron=health_cron,
        tags=["siamquantum", "health"],
    )
    prefect_serve(refresh_dep, health_dep)


@orch_app.command("deploy")
def cmd_deploy(
    pool: str = typer.Option("local-process", "--pool", help="Work pool name"),
    refresh_cron: str = typer.Option(
        "0 1 * * *", "--refresh-cron", help="Cron for daily refresh (UTC)"
    ),
    health_cron: str = typer.Option(
        "0 * * * *", "--health-cron", help="Cron for hourly healthcheck (UTC)"
    ),
) -> None:
    """Deploy flows to a running Prefect server with a work pool."""
    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "prefect_deploy_local.py"),
            "--pool",
            pool,
            "--refresh-cron",
            refresh_cron,
            "--health-cron",
            health_cron,
        ],
        cwd=REPO_ROOT,
    )
    raise typer.Exit(result.returncode)


@orch_app.command("worker")
def cmd_worker(
    pool: str = typer.Option("local-process", "--pool", help="Work pool name"),
) -> None:
    """Start a local-process Prefect worker attached to a work pool."""
    typer.echo(f"Starting Prefect worker on pool='{pool}' (Ctrl+C to stop)…")
    result = subprocess.run(
        [sys.executable, "-m", "prefect", "worker", "start", "--pool", pool]
    )
    raise typer.Exit(result.returncode)
