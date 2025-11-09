.PHONY: help verify install install-dev test test-all clean lint format run

verify:
	python verify_setup.py

help:
	@echo "Financial Reporting System - Make Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make verify        Verify setup and dependencies"
	@echo "  make install       Install project with uv"
	@echo "  make install-dev   Install with dev dependencies"
	@echo ""
	@echo "Testing:"
	@echo "  make test          Run quick module validator"
	@echo "  make test-all      Run all module tests"
	@echo "  make test-clean    Clean test outputs"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint          Run ruff linter"
	@echo "  make format        Format code with black"
	@echo ""
	@echo "Running:"
	@echo "  make run           Generate sample report"
	@echo "  make clean         Clean generated files"

install:
	uv pip install -e .

install-dev:
	uv pip install -e ".[dev]"

test:
	python quick_module_validator.py

test-all:
	python test/telco_test_module.py --all

test-clean:
	python test/telco_test_module.py --clean

lint:
	ruff check .

format:
	black .

run:
	python telco_modular_implementation.py --sample-data --render

clean:
	rm -rf reports/modular/*.html
	rm -rf reports/modular/*.pdf
	rm -rf test/module_tests/*.html
	rm -rf test/module_tests/*.qmd
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
