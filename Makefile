.PHONY: install db ingest-historical serve test lint

install:
	pip install -e ".[dev]"

db:
	python scripts/init_db.py

ingest-historical:
	@for year in 2020 2021 2022 2023 2024; do \
		python -m siamquantum ingest gdelt --year $$year; \
		python -m siamquantum ingest youtube --year $$year; \
	done

serve:
	python -m siamquantum serve

test:
	pytest

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/
	mypy src/siamquantum
