"""
Shared utility functions for financial reporting.
Used by both profit_loss_mapping.py and assets_mapping.py
"""

import polars as pl
from typing import List, Optional


def create_two_column_structure(data, mapping, total_only_categories=None):
    """
    Create the two-column structure used by both P&L and balance sheet.
    - Left column: detail/component amounts
    - Right column: subtotals and main totals
    
    Args:
        data: DataFrame with financial data
        mapping: DataFrame with level mapping
        total_only_categories: Categories that go directly to total column
    
    Returns:
        Tuple of (transformed_data, combined_data, year_columns, level_columns, sort_columns)
    """
    
    if total_only_categories is None:
        total_only_categories = []
    
    # Join the data with mapping
    combined = data.join(mapping, on='Categorie', how='left')
    combined = combined.filter(pl.col("Level0").is_not_null())

    # Identify different types of columns dynamically from the actual data
    all_columns = combined.columns
    level_columns = [col for col in all_columns if col.startswith('Level')]
    sort_columns = [col for col in all_columns if col.startswith('Sort')]
    
    # Identify year columns - check if they already have _detail/_total format
    excluded_cols = ['Categorie', 'Note'] + level_columns + sort_columns
    potential_year_columns = [col for col in all_columns if col not in excluded_cols]
    
    # Check if columns already have _detail/_total format
    detail_cols = [col for col in potential_year_columns if col.endswith('_detail')]
    total_cols = [col for col in potential_year_columns if col.endswith('_total')]
    
    if detail_cols and total_cols:
        # Data already has the correct format - extract base year names
        year_columns = []
        for detail_col in detail_cols:
            base_year = detail_col.replace('_detail', '')
            corresponding_total = f"{base_year}_total"
            if corresponding_total in total_cols:
                year_columns.append(base_year)
        
        # Transform original data: apply total_only_categories logic to existing columns
        original_transformed = combined.with_columns([
            pl.col("Categorie").is_in(total_only_categories).alias("is_total_only")
        ])
        
        # For each year, adjust the detail/total columns based on total_only_categories
        for year in year_columns:
            detail_col = f"{year}_detail"
            total_col = f"{year}_total"
            
            original_transformed = original_transformed.with_columns([
                # If category is total_only, move detail value to total and set detail to null
                pl.when(pl.col("is_total_only") & pl.col(detail_col).is_not_null())
                .then(pl.col(detail_col))
                .otherwise(pl.col(total_col))
                .cast(pl.Float64)
                .alias(total_col),
                
                pl.when(pl.col("is_total_only"))
                .then(pl.lit(None).cast(pl.Float64))
                .otherwise(pl.col(detail_col))
                .cast(pl.Float64)
                .alias(detail_col)
            ])
        
        original_transformed = original_transformed.drop(["is_total_only"])
        
    else:
        # Original logic for when columns are just year names
        year_columns = potential_year_columns
        
        # Transform original data: decide whether values go to detail or total columns
        original_transformed = combined.with_columns([
            pl.col("Categorie").is_in(total_only_categories).alias("is_total_only")
        ]).with_columns([
            *[pl.when(pl.col("is_total_only"))
              .then(pl.lit(None).cast(pl.Float64))
              .otherwise(pl.col(year_col))
              .cast(pl.Float64)
              .alias(f"{year_col}_detail") for year_col in year_columns],
            *[pl.when(pl.col("is_total_only"))
              .then(pl.col(year_col))
              .otherwise(pl.lit(None).cast(pl.Float64))
              .cast(pl.Float64)
              .alias(f"{year_col}_total") for year_col in year_columns],
        ]).drop(year_columns + ["is_total_only"])
    
    # Add metadata columns
    original_transformed = original_transformed.with_columns([
        pl.lit("original").cast(pl.String).alias("row_type"),
        pl.lit(None).cast(pl.String).alias("subtotal_level"),
        pl.lit(999).cast(pl.Int64).alias("sort_order")
    ])
    
    # Ensure all level and sort columns are cast to String for consistency
    for col in level_columns + sort_columns:
        if col in original_transformed.columns:
            original_transformed = original_transformed.with_columns(
                pl.col(col).cast(pl.String)
            )
    
    return original_transformed, combined, year_columns, level_columns, sort_columns


def calculate_subtotals_by_levels(combined_data, year_columns, level_columns, sort_columns, total_only_categories=None):
    """
    Calculate subtotals for all hierarchy levels.
    
    Args:
        combined_data: Combined data with mapping
        year_columns: List of year column names (base names like '2023', '2022')
        level_columns: List of level column names (e.g., ['Level0', 'Level1', 'Level2'])
        sort_columns: List of sort column names (e.g., ['Sort1', 'Sort2'])
        total_only_categories: Categories to exclude from subtotal calculations
    
    Returns:
        List of subtotal DataFrames
    """
    
    if total_only_categories is None:
        total_only_categories = []
    
    subtotal_rows = []
    
    # Check if we have detail/total format or just year columns
    has_detail_total = f"{year_columns[0]}_detail" in combined_data.columns
    
    for level_idx, level in enumerate(level_columns):
        group_cols = level_columns[:level_idx + 1]
        
        # Skip if any grouping column is null
        valid_data = combined_data.filter(
            pl.all_horizontal([pl.col(col).is_not_null() for col in group_cols])
        )
        
        if valid_data.height == 0:
            continue
            
        # Only sum detail items for subtotals (exclude total-only categories)
        detail_data = valid_data.filter(
            ~pl.col("Categorie").is_in(total_only_categories)
        )
        
        if detail_data.height == 0:
            continue
            
        # Build aggregation expressions based on data format
        if has_detail_total:
            # Sum from detail columns
            agg_expressions = [pl.sum(f"{year}_detail").alias(f"{year}_detail") for year in year_columns]
        else:
            # Sum from year columns directly
            agg_expressions = [pl.sum(col).alias(col) for col in year_columns]
        
        # Calculate subtotals for current level
        subtotals = detail_data.group_by(group_cols).agg(agg_expressions)
        
        # Transform subtotals: null in detail columns, values in total columns
        subtotal_transforms = [
            pl.col(group_cols[-1]).cast(pl.String).alias("Categorie"),
            pl.lit(None).cast(pl.String).alias("Note"),
            pl.lit("subtotal").cast(pl.String).alias("row_type"),
            pl.lit(level).cast(pl.String).alias("subtotal_level"),
            pl.lit(level_idx).cast(pl.Int64).alias("sort_order"),
        ]
        
        # Add year column transforms
        if has_detail_total:
            for year in year_columns:
                subtotal_transforms.extend([
                    pl.lit(None).cast(pl.Float64).alias(f"{year}_detail"),
                    pl.col(f"{year}_detail").cast(pl.Float64).alias(f"{year}_total")
                ])
        else:
            for year in year_columns:
                subtotal_transforms.extend([
                    pl.lit(None).cast(pl.Float64).alias(f"{year}_detail"),
                    pl.col(year).cast(pl.Float64).alias(f"{year}_total")
                ])
        
        # Add missing level and sort columns
        for col in level_columns:
            if col not in group_cols:
                subtotal_transforms.append(pl.lit(None).cast(pl.String).alias(col))
        
        for col in sort_columns:
            subtotal_transforms.append(pl.lit(None).cast(pl.String).alias(col))
        
        subtotal_df = subtotals.with_columns(subtotal_transforms)
        
        # Drop original year columns if we had detail/total format
        if has_detail_total:
            drop_cols = [f"{year}_detail" for year in year_columns if f"{year}_detail" in subtotal_df.columns]
            if drop_cols:
                # Only drop columns that exist and aren't being kept
                cols_to_drop = []
                for col in drop_cols:
                    # Don't drop if this column name is being used in the final output
                    if col not in [f"{year}_detail" for year in year_columns]:
                        cols_to_drop.append(col)
                if cols_to_drop:
                    subtotal_df = subtotal_df.drop(cols_to_drop)
        else:
            subtotal_df = subtotal_df.drop(year_columns)
        
        subtotal_rows.append(subtotal_df)
    
    return subtotal_rows


def sort_financial_data(df, level_columns, sort_columns):
    """
    Sort financial data maintaining logical hierarchy order.
    Uses Level columns first, then Sort columns for proper ordering.
    
    Args:
        df: DataFrame with Level columns, Sort columns, and sort_order
        level_columns: List of actual level column names
        sort_columns: List of actual sort column names (Sort1, Sort2, etc.)
    
    Returns:
        Sorted DataFrame
    """
    sort_cols = []
    
    # Add level columns for primary sorting
    for level in level_columns:
        sort_cols.append(pl.col(level).fill_null(""))
    
    # Add sort columns for secondary sorting (preserving alphanumeric data)
    for sort_col in sort_columns:
        sort_cols.append(pl.col(sort_col).fill_null(""))
    
    # Add final sorting columns
    sort_cols.extend([pl.col("sort_order"), pl.col("Categorie")])
    
    return df.sort(sort_cols)


def calculate_base_subtotals(data, mapping, total_only_categories=None):
    """
    Base function to calculate subtotals that can be used by both P&L and balance sheet.
    
    Args:
        data: Financial data DataFrame
        mapping: Mapping DataFrame
        total_only_categories: Categories that go to total column
    
    Returns:
        Tuple of (result_df, year_columns)
    """
    
    # Create two-column structure
    original_transformed, combined, year_columns, level_columns, sort_columns = create_two_column_structure(
        data, mapping, total_only_categories
    )
    
    # Calculate subtotals
    subtotal_dfs = calculate_subtotals_by_levels(
        combined, year_columns, level_columns, sort_columns, total_only_categories
    )
    
    # Combine all rows
    all_rows = [original_transformed] + subtotal_dfs
    result = pl.concat(all_rows, how="diagonal")
    
    # Sort and clean up
    result = sort_financial_data(result, level_columns, sort_columns)
    result = result.drop(["sort_order"])
    
    return result, year_columns


def format_for_display(df, year_columns):
    """
    Format the dataframe for display by combining detail and total columns.
    This function can be shared between profit_loss.py and assets.py
    """
    display_columns = ["Categorie", "Note"]
    
    for year_col in year_columns:
        detail_col = f"{year_col}_detail" 
        total_col = f"{year_col}_total"
        
        # Combine detail and total columns - show total if available, otherwise detail
        combined_col = pl.coalesce([pl.col(total_col), pl.col(detail_col)]).alias(year_col)
        display_columns.append(year_col)
        
        df = df.with_columns(combined_col)
    
    return df.select(display_columns + ["row_type", "subtotal_level"])


def filter_by_row_type(df, row_types):
    """
    Filter dataframe by row types.
    This function can be shared between profit_loss.py and assets.py
    
    Args:
        df: DataFrame with row_type column
        row_types: List of row types to keep (e.g., ['original', 'subtotal'])
    """
    return df.filter(pl.col("row_type").is_in(row_types))


def get_subtotals_only(df):
    """
    Get only subtotal rows.
    This function can be shared between profit_loss.py and assets.py
    """
    return filter_by_row_type(df, ["subtotal"])


def get_originals_only(df):
    """
    Get only original data rows.
    This function can be shared between profit_loss.py and assets.py
    """
    return filter_by_row_type(df, ["original"])


def get_calculated_only(df):
    """Get only calculated rows."""
    return filter_by_row_type(df, ["calculated"])


def get_value_from_df(df, category_name: str, year_col: str, 
                      row_type: str = "original", column_type: str = "total"):
    """
    Helper function to extract specific values from the financial DataFrame.
    
    Args:
        df: Financial DataFrame
        category_name: Category to look for
        year_col: Year column (e.g., '2023')
        row_type: Type of row ('original', 'subtotal', 'calculated')
        column_type: 'detail' or 'total'
    
    Returns:
        Numeric value or 0 if not found
    """
    col_name = f"{year_col}_{column_type}"
    rows = df.filter(
        (pl.col("Categorie") == category_name) & 
        (pl.col("row_type") == row_type)
    )
    
    if rows.height > 0:
        value = rows.select(pl.col(col_name)).item(0, 0)
        return value if value is not None else 0
    return 0


def add_calculation_rows(df, calculations: List[dict]):
    """
    Add calculated rows to the DataFrame.
    
    Args:
        df: Existing DataFrame
        calculations: List of calculation dictionaries
    
    Returns:
        DataFrame with calculations added
    """
    if not calculations:
        return df
    
    # Get all columns from df to ensure proper schema
    all_columns = df.columns
    
    # Create calculation DataFrame with proper column order
    calc_df_data = {}
    for col in all_columns:
        if col in calculations[0]:
            calc_df_data[col] = [row.get(col) for row in calculations]
        else:
            calc_df_data[col] = [None] * len(calculations)
    
    calc_df = pl.DataFrame(calc_df_data)
    return pl.concat([df, calc_df], how="diagonal")