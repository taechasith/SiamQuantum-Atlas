.PHONY: install db ingest-historical serve test lint

install:
	pip install -e ".[dev]"

db:
	python -m siamquantum db init

ingest-historical:
	python -m siamquantum ingest gdelt --year 2024 --all-years
	python -m siamquantum ingest youtube --year 2024 --all-years
	python -m siamquantum ingest geo --pending

serve:
	python -m siamquantum serve

test:
	pytest

lint:
	ruff check src tests
	ruff format --check src tests
