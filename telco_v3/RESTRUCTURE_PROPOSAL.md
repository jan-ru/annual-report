# Project Restructure Proposal

## Current Structure Issues

The project currently has 8 Python files in the root directory, making it cluttered and unclear which files are:
- Core library code
- CLI tools
- Utilities
- Scripts

## Proposed Structure

```
financial-reporting-system/
├── pyproject.toml
├── README.md
├── Makefile
├── .python-version
├── uv.lock
├── version.py                    # Keep in root (used by pyproject.toml)
│
├── src/                          # NEW: Source code package
│   └── financial_reporting/      # NEW: Main package
│       ├── __init__.py
│       ├── core/                 # Core processing logic
│       │   ├── __init__.py
│       │   ├── xaf_processor.py       # From main_polars_pivot.py
│       │   ├── data_processor.py      # From main_polars_pivot.py
│       │   └── calculations.py        # From main_polars_pivot.py
│       │
│       ├── reporting/            # Report generation
│       │   ├── __init__.py
│       │   ├── modular_system.py      # From modular_reporting_system.py
│       │   └── template_generator.py  # From modular_reporting_system.py
│       │
│       └── cli/                  # Command-line interfaces
│           ├── __init__.py
│           ├── telco_report.py        # From telco_modular_implementation.py
│           └── validator.py           # From quick_module_validator.py
│
├── scripts/                      # NEW: Utility scripts
│   ├── verify_setup.py           # From root
│   └── setup_test_env.sh         # From root
│
├── data/                         # Renamed from _data
│   └── account_mapping.csv       # From root
│
├── test/                         # Existing test directory
│   ├── __init__.py               # NEW
│   ├── test_xaf_processor.py     # NEW: Unit tests
│   ├── test_data_processor.py    # NEW: Unit tests
│   └── module_tests/             # Existing integration tests
│
├── reports/                      # Existing reports directory
├── docs/                         # Existing documentation
└── .kiro/                        # Existing Kiro config
```

## Detailed File Mapping

### Core Processing (`src/financial_reporting/core/`)

**xaf_processor.py** (from main_polars_pivot.py)
- `XAFProcessor` class
- XAF file parsing logic
- XML handling

**data_processor.py** (from main_polars_pivot.py)
- `ReportDataProcessor` class
- `create_comprehensive_income_statement()`
- `create_balance_sheet_mapping()`
- Data transformation functions

**calculations.py** (from main_polars_pivot.py)
- `FinancialCalculations` class
- `DutchGAAPBusinessRules` class
- Financial metrics and ratios

### Reporting (`src/financial_reporting/reporting/`)

**modular_system.py** (from modular_reporting_system.py)
- `ModularReportOrchestrator` class
- Report coordination logic

**template_generator.py** (from modular_reporting_system.py)
- `ReportTemplateGenerator` class
- QMD template generation
- Module file generation

### CLI (`src/financial_reporting/cli/`)

**telco_report.py** (from telco_modular_implementation.py)
- Main CLI entry point
- Argument parsing
- Report generation workflow

**validator.py** (from quick_module_validator.py)
- Module validation logic
- Quick test runner

### Scripts (`scripts/`)

**verify_setup.py** (from root)
- Environment verification
- Dependency checking

**setup_test_env.sh** (from root)
- Test environment setup

### Data (`data/`)

**account_mapping.csv** (from root)
- Account categorization mapping

## Benefits

### 1. **Cleaner Root Directory**
- Only essential files (pyproject.toml, README, Makefile)
- Clear separation of concerns
- Professional appearance

### 2. **Proper Python Package Structure**
- Installable as a package: `pip install -e .`
- Importable: `from financial_reporting.core import XAFProcessor`
- Better IDE support and autocomplete

### 3. **Easier Testing**
- Unit tests alongside code
- Clear test organization
- Better test discovery

### 4. **Better Maintainability**
- Logical grouping of related code
- Easier to find specific functionality
- Clear module boundaries

### 5. **Scalability**
- Easy to add new modules
- Clear place for new features
- Supports growth

## Migration Strategy

### Phase 1: Create Structure (No Breaking Changes)
1. Create `src/financial_reporting/` directory structure
2. Copy files to new locations (keep originals)
3. Update imports in new files
4. Add `__init__.py` files

### Phase 2: Update Configuration
1. Update `pyproject.toml` to point to new structure
2. Update `Makefile` paths
3. Update import statements in tests

### Phase 3: Verify & Test
1. Run all tests with new structure
2. Verify CLI commands work
3. Test report generation

### Phase 4: Clean Up
1. Remove old files from root
2. Update documentation
3. Commit changes

## Updated pyproject.toml

```toml
[project]
name = "financial-reporting-system"
version = "0.3.0"
# ... existing config ...

[project.scripts]
telco-report = "financial_reporting.cli.telco_report:main"
validate-modules = "financial_reporting.cli.validator:main"

[tool.hatch.build.targets.wheel]
packages = ["src/financial_reporting"]

[tool.pytest.ini_options]
testpaths = ["test"]
pythonpath = ["src"]
```

## Updated Imports

### Before
```python
from main_polars_pivot import XAFProcessor
from modular_reporting_system import ModularReportOrchestrator
```

### After
```python
from financial_reporting.core import XAFProcessor
from financial_reporting.reporting import ModularReportOrchestrator
```

## Backward Compatibility

To maintain backward compatibility during transition:

1. Keep old files temporarily with deprecation warnings
2. Add import redirects in root `__init__.py`
3. Update documentation with migration guide

## Files to Keep in Root

- `pyproject.toml` - Project configuration
- `README.md` - Main documentation
- `Makefile` - Task automation
- `.python-version` - Python version
- `uv.lock` - Dependency lock file
- `.gitignore` - Git ignore patterns
- `version.py` - Version info (referenced by pyproject.toml)

## Files to Remove from Root

- `main_polars_pivot.py` → `src/financial_reporting/core/`
- `modular_reporting_system.py` → `src/financial_reporting/reporting/`
- `telco_modular_implementation.py` → `src/financial_reporting/cli/`
- `quick_module_validator.py` → `src/financial_reporting/cli/`
- `telco_enhanced_modules.py` → Archive or remove (if unused)
- `verify_setup.py` → `scripts/`
- `setup_test_env.sh` → `scripts/`
- `account_mapping.csv` → `data/`
- `requirements.txt.backup` → `docs/` (already done)

## Implementation Checklist

- [ ] Create `src/financial_reporting/` directory structure
- [ ] Split `main_polars_pivot.py` into core modules
- [ ] Move `modular_reporting_system.py` to reporting/
- [ ] Move CLI files to cli/
- [ ] Create `__init__.py` files with proper exports
- [ ] Update `pyproject.toml` configuration
- [ ] Update imports in all files
- [ ] Move scripts to `scripts/`
- [ ] Move data files to `data/`
- [ ] Update Makefile paths
- [ ] Update documentation
- [ ] Run full test suite
- [ ] Update .gitignore if needed
- [ ] Commit changes

## Estimated Effort

- **Time:** 2-3 hours
- **Risk:** Low (can be done incrementally)
- **Testing:** Medium (need to verify all imports)
- **Documentation:** Low (mainly path updates)

## Recommendation

**Proceed with restructure** because:
1. Project is still small enough to refactor easily
2. Will prevent technical debt accumulation
3. Makes the project more professional
4. Easier for new contributors
5. Better IDE support and tooling

## Alternative: Minimal Restructure

If full restructure is too much, a minimal approach:

```
financial-reporting-system/
├── pyproject.toml
├── README.md
├── Makefile
├── version.py
│
├── financial_reporting/         # NEW: Just one package directory
│   ├── __init__.py
│   ├── xaf_processor.py
│   ├── data_processor.py
│   ├── modular_system.py
│   └── cli.py
│
├── scripts/
│   ├── verify_setup.py
│   └── telco_report.py          # Main CLI script
│
├── data/
│   └── account_mapping.csv
│
├── test/
└── reports/
```

This is simpler but still provides most benefits.

## Next Steps

1. Review this proposal
2. Choose between full or minimal restructure
3. Create a new branch for restructure work
4. Implement changes incrementally
5. Test thoroughly
6. Merge when stable

Would you like me to proceed with implementation?
