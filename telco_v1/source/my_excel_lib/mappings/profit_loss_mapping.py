"""
Profit & Loss statement processing module.

Calculate hierarchical subtotals for P&L statements with two-column format:
- Left column: detail/component amounts
- Right column: subtotals and main totals
"""

import polars as pl
from my_excel_lib.mappings.report_utils import (
    calculate_base_subtotals,
    get_value_from_df, 
    add_calculation_rows,
    format_for_display
)


def calculate_subtotals_two_column_format(data, mapping, total_only_categories=None):
    """
    Calculate subtotals for financial reports with two-column format.
    
    Args:
        data: DataFrame with columns like ['Categorie', 'Note', '2023', '2022', etc.]
        mapping: DataFrame with columns ['Level0', 'Level1', 'Level2', 'Level3', 'Categorie']
        total_only_categories: List of categories that should go directly to total column
    
    Returns:
        DataFrame with original data plus subtotal rows, properly formatted with 
        separate detail and total columns
    """
    
    if total_only_categories is None:
        total_only_categories = [
            'Netto-omzet', 
            'Financiele baten en lasten',
            'Belastingen',
            'Resultaat deelnemingen'
        ]
    
    # Use shared base function
    result, year_columns = calculate_base_subtotals(
        data, mapping, total_only_categories
    )
    
    return result


def handle_special_calculations(result_df, year_columns):
    """
    Handle special calculations for profit & loss statements:
    - Bedrijfsresultaat = Netto-omzet - Total Kosten
    - Resultaat voor belastingen = Bedrijfsresultaat + Financiele baten en lasten
    - Resultaat na belastingen = Resultaat voor belastingen - Belastingen - Resultaat deelnemingen
    
    Args:
        result_df: DataFrame with detail and total columns
        year_columns: List of original year column names (e.g., ['2023', '2022'])
    
    Returns:
        DataFrame with special calculations added
    """
    
    calculation_rows = []
    
    for year_col in year_columns:
        detail_col = f"{year_col}_detail"
        total_col = f"{year_col}_total"
        
        def get_subtotal_value(subtotal_name):
            """Get value from subtotal rows"""
            col_name = f"{year_col}_total"
            rows = result_df.filter(
                (pl.col("Categorie") == subtotal_name) & 
                (pl.col("row_type") == "subtotal")
            )
            if rows.height > 0:
                value = rows.select(pl.col(col_name)).item(0, 0)
                return value if value is not None else 0
            return 0
        
        # Calculate values using shared function
        netto_omzet = get_value_from_df(result_df, "Netto-omzet", year_col, "original", "total")
        kosten_subtotal = get_subtotal_value("Kosten")  # This would be the subtotal of all costs
        
        # If kosten_subtotal is 0, calculate it manually from components
        if kosten_subtotal == 0:
            personeelskosten = get_value_from_df(result_df, "Personeelskosten", year_col, "original", "detail")
            afschrijvingen = get_value_from_df(result_df, "Afschrijvingen", year_col, "original", "detail") 
            overige_kosten = get_value_from_df(result_df, "Overige bedrijfskosten", year_col, "original", "detail")
            kosten_subtotal = personeelskosten + afschrijvingen + overige_kosten
        
        bedrijfsresultaat = netto_omzet - kosten_subtotal
        
        financiele_baten = get_value_from_df(result_df, "Financiele baten en lasten", year_col, "original", "total")
        resultaat_voor_belastingen = bedrijfsresultaat + financiele_baten
        
        belastingen = get_value_from_df(result_df, "Belastingen", year_col, "original", "total")
        resultaat_deelnemingen = get_value_from_df(result_df, "Resultaat deelnemingen", year_col, "original", "total")
        resultaat_na_belastingen = resultaat_voor_belastingen - belastingen - resultaat_deelnemingen
        
        # Create calculation result rows
        calculations = [
            {
                "Categorie": "Bedrijfsresultaat",
                "Note": "",
                detail_col: None,
                total_col: bedrijfsresultaat,
                "Level0": None,
                "Level1": None,
                "Level2": None, 
                "Level3": None,
                "row_type": "calculated",
                "subtotal_level": "bedrijfsresultaat"
            },
            {
                "Categorie": "Resultaat voor belastingen", 
                "Note": "",
                detail_col: None,
                total_col: resultaat_voor_belastingen,
                "Level0": None,
                "Level1": None,
                "Level2": None, 
                "Level3": None,
                "row_type": "calculated",
                "subtotal_level": "resultaat_voor_belastingen"
            },
            {
                "Categorie": "Resultaat na belastingen",
                "Note": "",
                detail_col: None, 
                total_col: resultaat_na_belastingen,
                "Level0": None,
                "Level1": None,
                "Level2": None, 
                "Level3": None,
                "row_type": "calculated",
                "subtotal_level": "resultaat_na_belastingen"
            }
        ]
        
        calculation_rows.extend(calculations)
    
    # Use shared function to add calculations
    return add_calculation_rows(result_df, calculation_rows)


def calculate_subtotals(profit_loss_data, profit_loss_mapping):
    """
    Complete processing pipeline for profit & loss statement.
    
    Args:
        profit_loss_data: Raw P&L data
        profit_loss_mapping: P&L mapping with hierarchy levels
    
    Returns:
        Tuple of (final_df, display_df)
    """
    
    # Calculate subtotals with proper column placement
    result_df = calculate_subtotals_two_column_format(
        profit_loss_data, 
        profit_loss_mapping,
        total_only_categories=[
            'Netto-omzet', 
            'Financiele baten en lasten',
            'Belastingen',
            'Resultaat deelnemingen'
        ]
    )
    
    # Get year columns
    year_columns = [col for col in profit_loss_data.columns 
                   if col not in ['Categorie', 'Note']]
    
    # Add special calculations
    final_df = handle_special_calculations(result_df, year_columns)
    
    # Format for display
    display_df = format_for_display(final_df, year_columns)
    
    return final_df, display_df


# Example usage:
# final_df, display_df = process_profit_loss_report(profit_loss_data, profit_loss_mapping)
# 
# The result will include calculated rows for:
# - Bedrijfsresultaat 
# - Resultaat voor belastingen
# - Resultaat na belastingen