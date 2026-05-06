"""
main_polars_pivot — backward-compatibility shim.

All implementations have been moved to the financial_reporting package
under src/financial_reporting/. This module re-exports everything so that
existing scripts importing from this file continue to work unchanged.
"""

from financial_reporting.core.xaf_processor import XAFProcessor
from financial_reporting.core.data_processor import (
    create_income_statement_mapping,
    create_balance_sheet_mapping,
    fix_pivot_column_names,
    prepare_data_for_pivot,
    calculate_subtotals_with_pivot,
    calculate_base_subtotals_with_pivot,
    create_comprehensive_income_statement,
    XAFToFinancialStatements,
    ReportDataProcessor,
)
from financial_reporting.core.calculations import (
    CalculationResult,
    FinancialCalculations,
    RuleStatus,
    BusinessRuleResult,
    DutchGAAPBusinessRules,
    apply_calculations_with_pivot_results,
)

__all__ = [
    "XAFProcessor",
    "create_income_statement_mapping",
    "create_balance_sheet_mapping",
    "fix_pivot_column_names",
    "prepare_data_for_pivot",
    "calculate_subtotals_with_pivot",
    "calculate_base_subtotals_with_pivot",
    "create_comprehensive_income_statement",
    "XAFToFinancialStatements",
    "ReportDataProcessor",
    "CalculationResult",
    "FinancialCalculations",
    "RuleStatus",
    "BusinessRuleResult",
    "DutchGAAPBusinessRules",
    "apply_calculations_with_pivot_results",
]
