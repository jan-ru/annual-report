"""
Telco Report CLI

Main command-line interface for generating Telco B.V. financial reports.

This module currently imports from the legacy telco_modular_implementation module.
TODO: Refactor to standalone implementation.
"""

# Temporary: Import from legacy module
import sys
from pathlib import Path

# Add root to path to import legacy modules
root_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(root_path))

from telco_modular_implementation import (
    TelcoModularReportGenerator,
    fix_pivot_column_names,
    main,
)


__all__ = ["TelcoModularReportGenerator", "fix_pivot_column_names", "main"]
