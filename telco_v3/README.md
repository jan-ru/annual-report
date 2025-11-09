# Modular Financial Reporting System

**Version 0.3.0** - Dutch GAAP Automated Financial Reporting

A Python-based automated financial reporting system for Dutch companies, processing XAF audit files and generating professional annual reports.

## What's New in 0.3.0

- Fixed Great Tables styling API compatibility with version 0.18.0
- Improved test suite with cleaner output
- Enhanced module testing framework
- **Migrated to pyproject.toml + uv** for modern package management
- Added Makefile for common tasks
- Setup verification script

## Installation

### Using uv (Recommended - Fast!)
```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Set up project
uv venv
source .venv/bin/activate
uv pip install -e .
```

### Using pip (Traditional)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

See [MIGRATION_UV.md](MIGRATION_UV.md) for detailed migration guide.

## Quick Start

### Basic usage with sample data
python telco_modular_implementation.py --sample-data --render

# Generate multiple formats
python telco_modular_implementation.py --sample-data --render --formats html pdf

# Use your XAF file (when available)
python telco_modular_implementation.py --xaf-file audit_2024.xaf --render

# Custom output directory
python telco_modular_implementation.py --sample-data --output-dir ./custom_reports --render

# Debug mode
python telco_modular_implementation.py --sample-data --render --debug


## Quick Reference

### Verify Setup
```bash
make verify          # Check environment and dependencies
python verify_setup.py
```

### Common Tasks
```bash
make install         # Install dependencies
make test           # Quick validation
make test-all       # Run all tests
make run            # Generate sample report
make clean          # Clean generated files
```

See `make help` for all available commands.

## Testing

### Run all module tests
```bash
python test/telco_test_module.py --all
```

### Test specific modules
```bash
python test/telco_test_module.py --income_statement --render
python test/telco_test_module.py --balance_sheet --render
```

### Clean test outputs
```bash
python test/telco_test_module.py --clean
```

### Quick validation
```bash
python quick_module_validator.py
```

## Project Status

**Completed:**
- ✅ Modular architecture with separated data processing and presentation
- ✅ Great Tables styling API compatibility
- ✅ Comprehensive test suite with 8 modules
- ✅ Test output cleanup command
- ✅ All tests passing

**To Do:**
- [ ] Initiate local and remote git repositories
- [ ] Adjust tests to test the modules under reports
- [ ] Read XAF file (separate module)
- [ ] Improve README (badges + images)
- [ ] Put shared test code in separate module
- [ ] Start DuckDB top layer for CUBE functions

## Version History

- **0.3.0** (2024-11-07): Fixed Great Tables styling API compatibility, improved test suite
- **0.2.0**: Modular architecture with separated data processing and presentation
- **0.1.0**: Initial XAF processing and Polars-based financial statement generation
