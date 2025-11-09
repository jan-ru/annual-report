# Project Structure

## Directory Organization

```
.
├── .venv/                          # Python virtual environment
├── _data/                          # Source data files
│   └── EBi - Amsterdam Overtoom.pdf
├── reports/                        # Generated reports
│   └── modular/                    # Modular report output
│       ├── processed_data/         # Cleaned data files (CSV, JSON)
│       ├── report_modules/         # Python modules for report sections
│       └── *.qmd                   # Quarto templates
├── test/                           # Testing framework
│   ├── module_tests/               # Individual module tests
│   │   ├── report_modules/         # Test copies of modules
│   │   ├── processed_data/         # Test data
│   │   └── test_*.py               # Test scripts per module
│   ├── telco_test_module.py        # Main test orchestrator
│   └── README.md                   # Test documentation
├── main_polars_pivot.py            # Core data processing engine
├── modular_reporting_system.py     # Modular architecture framework
├── telco_modular_implementation.py # Main CLI entry point
├── quick_module_validator.py       # Fast module validation
├── version.py                      # Version information (v0.2.0)
├── account_mapping.csv             # Account categorization mapping
└── requirements.txt                # Python dependencies
```

## Key Components

### Core Processing (`main_polars_pivot.py`)
Contains the data processing engine with:
- `XAFProcessor`: XML audit file parser
- `ReportDataProcessor`: Financial data transformation
- `FinancialCalculations`: Derived metrics and ratios
- `DutchGAAPBusinessRules`: Validation rules
- Polars-based pivot and aggregation functions

### Modular System (`modular_reporting_system.py`)
Defines the modular architecture:
- `ReportDataProcessor`: Processes income statements and balance sheets
- `ReportTemplateGenerator`: Creates QMD templates and Python modules
- `ModularReportOrchestrator`: Coordinates full report generation

### Main Entry Point (`telco_modular_implementation.py`)
CLI application with:
- `TelcoModularReportGenerator`: Main report generator class
- Sample data creation for Telco B.V.
- XAF file processing integration
- Quarto rendering orchestration

## Report Module Structure

Each report consists of 8 Python modules in `report_modules/`:

1. **setup.py**: Data loading, imports, helper functions
2. **executive_summary.py**: Key financial highlights
3. **income_statement.py**: P&L statement with Great Tables
4. **balance_sheet.py**: Balance sheet display
5. **financial_ratios.py**: Calculated metrics and KPIs
6. **revenue_charts.py**: Revenue visualization (Plotly)
7. **asset_charts.py**: Asset composition charts
8. **notes.py**: Methodology and documentation

### Processed Data Files

Located in `processed_data/`:
- `income_statement.csv`: Income statement data
- `balance_sheet.csv`: Balance sheet data
- `calculations.json`: Derived metrics
- `metadata.json`: Report metadata (company, year, generation date)
- `chart_data.json`: Pre-processed chart data

## Architectural Patterns

### Separation of Concerns
- **Data Processing**: Pure Python logic in `.py` files
- **Presentation**: Quarto templates (`.qmd`) with minimal embedded code
- **Data Storage**: Intermediate CSV/JSON files for clean handoff

### Module Independence
Each report module:
- Can be tested independently
- Has clear dependencies (documented in docstrings)
- Loads data from `processed_data/` via `setup.py`
- Produces self-contained output

### Testing Strategy
- **Quick Validation**: `quick_module_validator.py` for fast checks
- **Module Tests**: Individual test scripts per module in `test/module_tests/`
- **Integration Tests**: Full report generation via main CLI

## File Naming Conventions

- **Python modules**: `snake_case.py`
- **Report modules**: Descriptive names (`income_statement.py`, `balance_sheet.py`)
- **Test files**: `test_<module_name>.py`
- **QMD templates**: `<company>_<report_type>_<year>.qmd`
- **Data files**: Lowercase with underscores (`income_statement.csv`)

## Data Flow

1. **Input**: XAF file or sample data
2. **Processing**: `main_polars_pivot.py` transforms to financial statements
3. **Storage**: Save to `processed_data/` as CSV/JSON
4. **Module Generation**: Create Python modules in `report_modules/`
5. **Template Creation**: Generate QMD template referencing modules
6. **Rendering**: Quarto processes QMD → HTML/PDF/DOCX

## Important Directories

- **Never commit**: `.venv/`, `__pycache__/`, `*.pyc`
- **Generated outputs**: `reports/modular/`, `test/module_tests/test_*_report.*`
- **Source control**: All `.py` files, `requirements.txt`, `account_mapping.csv`
