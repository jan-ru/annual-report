#!/usr/bin/env python3
"""
Telco B.V. Financial Reporting - Balance Sheet Module

Description:
    Generates and displays the balance sheet (balans) with professional formatting 
    using Great Tables. Includes financial position analysis, working capital 
    calculations, and equity ratio assessment.

Dependencies:
    - pandas: DataFrame manipulation
    - great_tables: Professional table formatting
    - Required from setup: balance_df, current_year, previous_year, 
      format_currency_eur, get_safe_value

Output:
    - Formatted balance sheet table
    - Financial position analysis
    - Working capital and liquidity commentary

Created: 2024-08-04
Updated: 2024-08-04  
Version: 1.0.1
Author: Automated Financial Reporting System
"""

# Telco B.V. Balance Sheet Module

print("## Balance Sheet")
print()

# Create Great Tables version of balance sheet
balance_display_df = balance_df.copy()

# Ensure we have the right columns
year_columns = [col for col in balance_display_df.columns if col.isdigit()]
if not year_columns:
    year_columns = [col for col in balance_display_df.columns if current_year in col or previous_year in col]

print(f"📊 Balance sheet columns: {balance_display_df.columns.tolist()}")
print(f"📅 Year columns found: {year_columns}")

# Create the Great Table with error handling
try:
    balance_gt = (
        GT(balance_display_df)
        .tab_header(
            title="Balance Sheet (Balans)",
            subtitle=f"As of December 31, {current_year} and {previous_year} (in EUR)"
        )
        .cols_label(
            Categorie="Category"
        )
    )
    
    # Handle Note column if it exists
    if 'Note' in balance_display_df.columns:
        balance_gt = balance_gt.cols_label(Note="Note")
    
    # Format currency columns
    for col in year_columns:
        if col in balance_display_df.columns:
            try:
                balance_gt = balance_gt.fmt_currency(
                    columns=[col],
                    currency="EUR",
                    placement="left",
                    decimals=0
                )
            except Exception as e:
                print(f"⚠️ Currency formatting failed for {col}: {e}")
    
    # Style asset totals with updated syntax
    asset_categories = ['Totaal activa']
    for category in asset_categories:
        category_mask = balance_display_df['Categorie'] == category
        if category_mask.any():
            indices = balance_display_df.index[category_mask].tolist()
            try:
                from great_tables import loc, style
                balance_gt = balance_gt.tab_style(
                    style=[style.fill(color="#e6f3ff"), style.text(weight="bold"), style.borders(sides="top", color="black", weight="2px")],
                    locations=loc.body(rows=indices)
                )
            except Exception as e:
                print(f"⚠️ Asset total styling failed: {e}")
    
    # Style equity with updated syntax
    equity_categories = ['Eigen vermogen', 'Totaal passiva']
    for category in equity_categories:
        category_mask = balance_display_df['Categorie'] == category
        if category_mask.any():
            indices = balance_display_df.index[category_mask].tolist()
            try:
                from great_tables import loc, style
                balance_gt = balance_gt.tab_style(
                    style=[style.fill(color="#f0f8e6"), style.text(weight="bold")],
                    locations=loc.body(rows=indices)
                )
            except Exception as e:
                print(f"⚠️ Equity styling failed for {category}: {e}")
    
    # Display the table
    balance_gt.show()
    
except Exception as e:
    print(f"⚠️ Great Tables display failed: {e}")
    print("📊 Displaying raw data instead:")
    print(balance_display_df.to_string())

# Analysis
print()
print("### Balance Sheet Analysis")

real_estate = get_safe_value(balance_df, 'Vastgoedbeleggingen', current_year)
if real_estate > 0:
    print(f"The company invested {format_currency_eur(real_estate)} in real estate investments during {current_year}.")

fin_assets_current = get_safe_value(balance_df, 'Financiële vaste activa', current_year)
fin_assets_previous = get_safe_value(balance_df, 'Financiële vaste activa', previous_year)

if fin_assets_current != fin_assets_previous:
    change = fin_assets_current - fin_assets_previous
    if change < 0:
        print(f"Financial fixed assets decreased by {format_currency_eur(abs(change))} to {format_currency_eur(fin_assets_current)}.")
    else:
        print(f"Financial fixed assets increased by {format_currency_eur(change)} to {format_currency_eur(fin_assets_current)}.")

liquidity = get_safe_value(balance_df, 'Liquide middelen', current_year)
receivables = get_safe_value(balance_df, 'Vorderingen', current_year)
working_capital = liquidity + receivables - get_safe_value(balance_df, 'Kortlopende schulden', current_year)

print(f"Working capital amounts to {format_currency_eur(working_capital)}, ")
print(f"consisting of cash and equivalents ({format_currency_eur(liquidity)}) and receivables ({format_currency_eur(receivables)}).")

# Get equity data for ratio calculation
equity_current = get_safe_value(balance_df, 'Eigen vermogen', current_year)
total_assets_current = get_safe_value(balance_df, 'Totaal activa', current_year)

# If totals aren't available, calculate from components
if total_assets_current == 0:
    total_assets_current = (
        get_safe_value(balance_df, 'Vastgoedbeleggingen', current_year) +
        get_safe_value(balance_df, 'Financiële vaste activa', current_year) +
        get_safe_value(balance_df, 'Vorderingen', current_year) +
        get_safe_value(balance_df, 'Effecten', current_year) +
        get_safe_value(balance_df, 'Liquide middelen', current_year)
    )

equity_ratio = (equity_current / total_assets_current * 100) if total_assets_current > 0 else 0
print(f"The equity ratio is {equity_ratio:.1f}%, indicating a {'strong' if equity_ratio > 50 else 'adequate' if equity_ratio > 30 else 'weak'} financial position.")

print("✅ Balance sheet analysis completed")