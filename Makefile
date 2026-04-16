PYTHON ?= python

setup:
	$(PYTHON) main.py setup

demo:
	$(PYTHON) main.py demo

export-arena:
	$(PYTHON) main.py export-arena

report:
	$(PYTHON) main.py report

test:
	pytest

serve-viewer:
	$(PYTHON) viewer/server.py
