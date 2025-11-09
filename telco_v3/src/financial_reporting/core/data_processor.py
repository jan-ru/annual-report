"""
Data Processing Module

Handles financial data transformation, pivot operations, and statement generation.

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
    create_comprehensive_income_statement,
    create_balance_sheet_mapping,
    create_income_statement_mapping,
    prepare_data_for_pivot,
    calculate_subtotals_with_pivot,
    calculate_base_subtotals_with_pivot,
    apply_calculations_with_pivot_results,
)

from modular_reporting_system import ReportDataProcessor as LegacyReportDataProcessor


# Re-export for compatibility
def fix_pivot_column_names(df):
    """
    Fix problematic column names created by Polars pivot operations.
    
    Polars pivot can create complex JSON-like column names. This function
    cleans them up to create readable names like '2024_total' and '2024_detail'.
    """
    import polars as pl
    import re
    
    new_columns = {}
    
    for col in df.columns:
        if col == 'Categorie' or not isinstance(col, str):
            continue
            
        # Check if column name contains JSON-like structure
        if '{' in col and '}' in col:
            # Extract year
            year_match = re.search(r'"(\d{4})"', col)
            if year_match:
                year = year_match.group(1)
                
                # Determine if it's total or detail
                if '"total"' in col.lower():
                    new_name = f"{year}_total"
                elif '"detail"' in col.lower():
                    new_name = f"{year}_detail"
                else:
                    new_name = f"{year}"
                
                new_columns[col] = new_name
    
    if new_columns:
        df = df.rename(new_columns)
    
    return df


class ReportDataProcessor(LegacyReportDataProcessor):
    """
    Wrapper around legacy ReportDataProcessor.
    Maintains compatibility while allowing gradual refactoring.
    """
    pass


__all__ = [
    "create_comprehensive_income_statement",
    "create_balance_sheet_mapping",
    "create_income_statement_mapping",
    "prepare_data_for_pivot",
    "calculate_subtotals_with_pivot",
    "calculate_base_subtotals_with_pivot",
    "apply_calculations_with_pivot_results",
    "fix_pivot_column_names",
    "ReportDataProcessor",
]
