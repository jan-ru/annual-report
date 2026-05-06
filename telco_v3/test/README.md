# Telco Module Testing

This directory contains tools for testing individual report modules.

## Quick Start

```bash
# List available modules
python telco_test_module.py --list

# Test a specific module
python telco_test_module.py --income_statement

# Test with rendering
python telco_test_module.py --balance_sheet --render

# Test all modules
python telco_test_module.py --all --render

# Clean up
python telco_test_module.py --clean
```

## Module Testing Workflow

1. **Setup**: `python telco_test_module.py --setup`
2. **Test individual modules**: `python telco_test_module.py --income_statement`
3. **Render reports**: Add `--render` flag to create HTML output
4. **Test everything**: `python telco_test_module.py --all --render`

## Directory Structure

```
test/
├── telco_test_module.py          # Main testing script
├── quick_module_validator.py     # Quick validation
├── module_tests/                 # Generated test environment
│   ├── report_modules/           # All 8 Python modules
│   ├── processed_data/           # Sample data files
│   ├── test_income_statement.py  # Generated test scripts
│   ├── test_balance_sheet.py     # Generated test scripts
│   └── telco_bv_jaarrekening_2024.qmd  # Full report
└── README.md                     # Documentation
```

## Debugging Individual Modules

You can also test modules manually:

```bash
cd module_tests
python -c "
exec(open('report_modules/data_loader.py').read())
exec(open('report_modules/income_statement.py').read())
"
```
