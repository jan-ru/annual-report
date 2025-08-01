"""
Balance sheet assets processing module.

Calculate hierarchical subtotals for balance sheet assets with two-column format:
- Left column: detail/component amounts  
- Right column: subtotals and main totals
"""

import polars as pl
from my_excel_lib.mappings.report_utils import (
    calculate_base_subtotals,
    format_for_display,
    add_calculation_rows
)


def calculate_subtotals(assets_data, assets_mapping, total_only_categories=None):
    """
    Calculate subtotals for balance sheet assets with two-column format.
    
    Args:
        assets_data: DataFrame with columns like ['Categorie', 'Note', '2023', '2022', etc.]
        assets_mapping: DataFrame with columns ['Level0', 'Level1', 'Level2', 'Level3', 'Categorie']
        total_only_categories: List of categories that should go directly to total column
    
    Returns:
        DataFrame with original data plus subtotal rows, properly formatted with 
        separate detail and total columns
    """
    
    if total_only_categories is None:
        total_only_categories = [
            'Vastgoedbeleggingen',
            'Effecten', 
            'Liquide middelen',
            'Totaal actief'
        ]
    
    # Use shared base function
    result, year_columns = calculate_base_subtotals(
        assets_data, assets_mapping, total_only_categories
    )
    
    return result


def calculate_balance_sheet_totals(result_df, year_columns):
    """
    Calculate the final balance sheet totals:
    - Totaal actief = sum of all main categories (Vaste activa + Vlottende activa)
    
    Args:
        result_df: DataFrame with detail and total columns
        year_columns: List of original year column names (e.g., ['2023', '2022'])
    
    Returns:
        DataFrame with total calculations added
    """
    
    calculation_rows = []
    
    for year_col in year_columns:
        total_col = f"{year_col}_total"
        
        # Get main category subtotals (Level0 subtotals)
        main_subtotals = result_df.filter(
            (pl.col("row_type") == "subtotal") & 
            (pl.col("subtotal_level") == "Level0")
        )
        
        if main_subtotals.height > 0:
            total_actief = main_subtotals.select(pl.sum(pl.col(total_col))).item(0, 0)
            
            # Create total row
            total_row = {
                "Categorie": "Totaal actief",
                "Note": "",
                f"{year_col}_detail": None,
                total_col: total_actief,
                "Level0": None,
                "Level1": None,
                "Level2": None,
                "Level3": None,
                "row_type": "calculated",
                "subtotal_level": "total_actief"
            }
            
            calculation_rows.append(total_row)
    
    # Use shared function to add calculations
    return add_calculation_rows(result_df, calculation_rows)


def process_assets_report(assets_data, assets_mapping):
    """
    Complete processing pipeline for balance sheet assets.
    
    Args:
        assets_data: Raw assets data
        assets_mapping: Assets mapping with hierarchy levels
    
    Returns:
        Tuple of (final_df, display_df)
    """
    
    # Calculate subtotals with proper column placement
    result_df = calculate_balance_sheet_subtotals(
        assets_data, 
        assets_mapping,
        total_only_categories=[
            'Vastgoedbeleggingen',  # Goes to total column
            'Effecten',             # Goes to total column (under Vlottende activa)
            'Liquide middelen',     # Goes to total column
            'Totaal actief'         # Final total
        ]
    )
    
    # Get year columns
    year_columns = [col for col in assets_data.columns 
                   if col not in ['Categorie', 'Note']]
    
    # Add final calculations
    final_df = calculate_balance_sheet_totals(result_df, year_columns)
    
    # Format for display
    display_df = format_for_display(final_df, year_columns)
    
    return final_df, display_df