.PHONY: install dev test lint format typecheck clean build publish help

PYTHON := python3
UV := uv

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install the package
	$(UV) pip install -e .

dev:  ## Install the package with dev dependencies
	$(UV) venv
	$(UV) pip install -e ".[dev]"

test:  ## Run tests
	$(UV) run pytest tests/ -v

test-cov:  ## Run tests with coverage
	$(UV) run pytest tests/ --cov=trossen_cloud_cli --cov-report=term-missing --cov-report=html

lint:  ## Run linter
	$(UV) run ruff check src/trossen_cloud_cli tests

lint-fix:  ## Run linter and fix issues
	$(UV) run ruff check --fix src/trossen_cloud_cli tests

format:  ## Format code
	$(UV) run ruff format src/trossen_cloud_cli tests

format-check:  ## Check code formatting
	$(UV) run ruff format --check src/trossen_cloud_cli tests

typecheck:  ## Run type checker
	$(UV) run mypy src/trossen_cloud_cli

check:  ## Run all checks (lint, format, typecheck, test)
	@echo "Running linter..."
	$(UV) run ruff check src/trossen_cloud_cli tests
	@echo "Checking format..."
	$(UV) run ruff format --check src/trossen_cloud_cli tests
	@echo "Running type checker..."
	$(UV) run mypy src/trossen_cloud_cli
	@echo "Running tests..."
	$(UV) run pytest tests/ -v

clean:  ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

build:  ## Build the package
	$(UV) build
