"""
telco_modular_implementation — backward-compatibility shim.

All implementations have been moved to financial_reporting.cli.telco_report.
"""

from financial_reporting.cli.telco_report import (
    TelcoModularReportGenerator,
    main,
)
from financial_reporting.core.data_processor import fix_pivot_column_names

__all__ = [
    "TelcoModularReportGenerator",
    "fix_pivot_column_names",
    "main",
]

if __name__ == "__main__":
    import sys
    sys.exit(main())
