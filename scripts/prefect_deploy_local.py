#!/usr/bin/env python
"""Deploy siamquantum flows to a running Prefect server with a local-process work pool.

Prerequisites:
    prefect server start          # in a separate terminal
    prefect work-pool create --type local-process local-process

Usage:
    python scripts/prefect_deploy_local.py
    python scripts/prefect_deploy_local.py --pool my-pool --refresh-cron "0 2 * * *"
"""
from __future__ import annotations

import typer

app = typer.Typer()


@app.command()
def main(
    pool: str = typer.Option("local-process", "--pool"),
    refresh_cron: str = typer.Option("0 1 * * *", "--refresh-cron"),
    health_cron: str = typer.Option("0 * * * *", "--health-cron"),
) -> None:
    from siamquantum.orchestration.prefect_flows import healthcheck_flow, refresh_flow

    refresh_id = refresh_flow.deploy(
        name="siamquantum-refresh-daily",
        work_pool_name=pool,
        cron=refresh_cron,
        tags=["siamquantum", "refresh"],
    )
    typer.echo(f"Deployed siamquantum-refresh-daily  → {refresh_id}")

    health_id = healthcheck_flow.deploy(
        name="siamquantum-healthcheck-hourly",
        work_pool_name=pool,
        cron=health_cron,
        tags=["siamquantum", "health"],
    )
    typer.echo(f"Deployed siamquantum-healthcheck-hourly → {health_id}")
    typer.echo(f"\nStart a worker:  siamquantum orchestration worker --pool {pool}")
    typer.echo("View runs:       prefect server start  →  http://127.0.0.1:4200")


if __name__ == "__main__":
    app()
