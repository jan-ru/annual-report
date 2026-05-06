# Technology Stack

## Core Technologies

### Python Environment
- **Python 3.10+** (specified in `.python-version`)
- Package management via **uv** (modern, fast Python package manager)
- Project configuration in `pyproject.toml` (PEP 621 compliant)

### Data Processing
- **Polars**: Primary DataFrame library for efficient data manipulation and pivot operations
- **Pandas**: Used for Great Tables compatibility and legacy support
- **NumPy**: Numerical operations

### Reporting & Visualization
- **Quarto**: Document generation and rendering engine (QMD templates)
- **Great Tables**: Professional table formatting for financial statements
- **Plotly**: Interactive charts and visualizations (revenue trends, asset composition)

### Development Tools
- **Jupyter**: Interactive development and testing (`jupyter`, `ipykernel`, `jupyterlab`)
- **XML Processing**: Built-in `xml.etree.ElementTree` for XAF file parsing

## Project Structure

### Key Libraries
```
polars>=1.32.0          # DataFrame operations
pandas>=2.3.1           # Data manipulation
great-tables>=0.18.0    # Table formatting
plotly>=6.2.0           # Visualizations
jupyter>=1.1.1          # Development environment
```

All dependencies are managed in `pyproject.toml`

## Version

Current version: **0.2.0**

Check version: `python telco_modular_implementation.py --version`

## Common Commands

### Setup

#### Using uv (Recommended)
```bash
# Install uv if not already installed
# macOS/Linux: curl -LsSf https://astral.sh/uv/install.sh | sh
# Or via pip: pip install uv

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # macOS/Linux

# Install project with dependencies
uv pip install -e .

# Or sync from lock file (if available)
uv sync
```

#### Traditional pip (Alternative)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Running Reports

```bash
# Generate report with sample data
python telco_modular_implementation.py --sample-data --render

# Generate from XAF file
python telco_modular_implementation.py --xaf-file audit_2024.xaf --render

# Multiple output formats
python telco_modular_implementation.py --sample-data --render --formats html pdf

# Custom output directory
python telco_modular_implementation.py --sample-data --output-dir ./custom_reports --render
```

### Testing

```bash
# Quick module validation
python quick_module_validator.py

# Test specific module
cd test
python telco_test_module.py --income_statement --render

# Test all modules
python telco_test_module.py --all --render

# List available test modules
python telco_test_module.py --list

# Clean test outputs
python telco_test_module.py --clean
```

### Rendering

```bash
# Render Quarto document manually
cd reports/modular
quarto render telco_bv_jaarrekening_2024.qmd --to html
quarto render telco_bv_jaarrekening_2024.qmd --to pdf
```

## Important Notes

### Polars Deprecation Fixes
The codebase has been updated to fix Polars deprecation warnings:
- Use `on` parameter instead of `columns` in pivot operations
- Explicit type casting with `cast(pl.Float64, strict=False)` for schema alignment
- Proper column alignment before `pl.concat()` operations

### Column Naming
Pivot operations may create complex JSON-like column names. The `fix_pivot_column_names()` function handles cleanup to create readable names like `2024_total` and `2024_detail`.

### Module Execution
Report modules use `exec(open('module.py').read())` pattern in QMD templates for clean separation of logic and presentation.
