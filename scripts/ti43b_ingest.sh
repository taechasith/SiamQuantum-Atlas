#!/usr/bin/env bash
# TI-4.3b: Sequential Thai-first ingest, 2020-2025
set -e
cd "$(dirname "$0")/.."

for year in 2020 2021 2022 2023 2024 2025; do
    echo "=== YEAR $year ==="

    echo "[gdelt $year]"
    python -m siamquantum ingest gdelt --year $year
    echo "[sleep 60s before youtube...]"
    sleep 60

    echo "[youtube $year]"
    python -m siamquantum ingest youtube --year $year
    echo "[sleep 30s before next year...]"
    sleep 30
done

echo "=== INGEST COMPLETE ==="
