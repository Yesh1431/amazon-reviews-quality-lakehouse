PYTHON ?= python

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

test:
	pytest -q

run:
	$(PYTHON) -m src.cli --input-path data/raw --bronze-path data/bronze --silver-path data/silver --gold-path data/gold --quarantine-path data/quarantine

clean:
	rm -rf data/bronze/* data/silver/* data/gold/* data/quarantine/*
