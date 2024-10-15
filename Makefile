
all: clean test dists

install:
	pip install -q -e ".[dev]"

test:
	pytest

typecheck:
	mypy alephclient/	

dists:
	python setup.py sdist
	python setup.py bdist_wheel

release: dists
	pip install -q twine
	twine upload dist/*

clean:
	rm -rf dist build .eggs
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +

lint-check:
	ruff check

format-check:
	ruff format --check

format:
	ruff format
