.PHONY: install lint format test build clean

install:
	uv pip install -e ".[dev]"

lint:
	ruff check greennode_airflow_plugin tests
	mypy greennode_airflow_plugin

format:
	black greennode_airflow_plugin tests
	isort greennode_airflow_plugin tests

test:
	pytest

build:
	uv build || python -m build

clean:
	rm -rf dist build *.egg-info .pytest_cache .mypy_cache .ruff_cache
