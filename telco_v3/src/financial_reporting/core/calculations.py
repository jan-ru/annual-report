"""
Financial Calculations Module

Handles financial calculations, ratios, and Dutch GAAP business rules.

This module currently imports from the legacy main_polars_pivot module.
TODO: Refactor to standalone implementation.
"""

# Temporary: Import from legacy module
import sys
from pathlib import Path

# Add root to path to import legacy modules
root_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(root_path))

from main_polars_pivot import (
    FinancialCalculations,
    DutchGAAPBusinessRules,
    CalculationResult,
    RuleStatus,
    BusinessRuleResult,
)


__all__ = [
    "FinancialCalculations",
    "DutchGAAPBusinessRules",
    "CalculationResult",
    "RuleStatus",
    "BusinessRuleResult",
]
