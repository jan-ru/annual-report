# Telco B.V. Income Statement Module - FIXED

print("## Income Statement")
print()

# Create Great Tables version of income statement
income_display_df = income_df.copy()

# Ensure we have the right columns
year_columns = [col for col in income_display_df.columns if col.isdigit()]
if not year_columns:
    # Handle alternative column naming
    year_columns = [col for col in income_display_df.columns if current_year in col or previous_year in col]

print(f"📊 Income statement columns: {income_display_df.columns.tolist()}")
print(f"📅 Year columns found: {year_columns}")

# Create the Great Table with error handling
try:
    income_gt = (
        GT(income_display_df)
        .tab_header(
            title="Income Statement (Winst-en-verliesrekening)",
            subtitle=f"Years ending December 31, {current_year} and {previous_year} (in EUR)"
        )
        .cols_label(
            Categorie="Category"
        )
    )
    
    # Handle Note column if it exists
    if 'Note' in income_display_df.columns:
        income_gt = income_gt.cols_label(Note="Note")
    
    # Format currency columns
    for col in year_columns:
        if col in income_display_df.columns:
            try:
                income_gt = income_gt.fmt_currency(
                    columns=[col],
                    currency="EUR",
                    placement="left",
                    decimals=0
                )
            except Exception as e:
                print(f"⚠️ Currency formatting failed for {col}: {e}")
    
    # Style revenue row with updated syntax
    revenue_mask = income_display_df['Categorie'] == 'Netto-omzet'
    if revenue_mask.any():
        revenue_indices = income_display_df.index[revenue_mask].tolist()
        try:
            from great_tables import loc, style
            income_gt = income_gt.tab_style(
                style=[style.fill(color="#e6f3ff"), style.text(weight="bold")],
                locations=loc.body(rows=revenue_indices)
            )
        except Exception as e:
            print(f"⚠️ Revenue row styling failed: {e}")
    
    # Style result rows with updated syntax
    result_categories = ['Bedrijfsresultaat', 'Resultaat voor belastingen', 'Resultaat na belastingen']
    for category in result_categories:
        category_mask = income_display_df['Categorie'] == category
        if category_mask.any():
            indices = income_display_df.index[category_mask].tolist()
            try:
                from great_tables import loc, style
                income_gt = income_gt.tab_style(
                    style=[style.fill(color="#f0f8e6"), style.text(weight="bold")],
                    locations=loc.body(rows=indices)
                )
            except Exception as e:
                print(f"⚠️ Result row styling failed for {category}: {e}")
    
    # Add top border
    try:
        from great_tables import loc, style
        income_gt = income_gt.tab_style(
            style=style.borders(sides="top", color="black", weight="2px"),
            locations=loc.body(rows=[0])
        )
    except Exception as e:
        print(f"⚠️ Border styling failed: {e}")
    
    # Display the table
    income_gt.show()
    
except Exception as e:
    print(f"⚠️ Great Tables display failed: {e}")
    print("📊 Displaying raw data instead:")
    print(income_display_df.to_string())

# Analysis
print()
print("### Income Statement Analysis")

# Get revenue data for analysis
revenue_current = get_safe_value(income_df, 'Netto-omzet', current_year)
revenue_previous = get_safe_value(income_df, 'Netto-omzet', previous_year)

if revenue_current > 0:
    print(f"The company generated revenue of {format_currency_eur(revenue_current)} in {current_year}, ")
    if revenue_previous == 0:
        print("representing the establishment of new revenue streams.")
    else:
        change = revenue_current - revenue_previous
        print(f"compared to {format_currency_eur(revenue_previous)} in {previous_year}, ")
        print(f"representing a change of {format_currency_eur(change)}.")

personnel_costs = get_safe_value(income_df, 'Personeelskosten', current_year)
if personnel_costs != 0:
    print(f"Personnel costs for {current_year} were {format_currency_eur(abs(personnel_costs))}.")

operating_costs = get_safe_value(income_df, 'Overige bedrijfskosten', current_year)
if operating_costs != 0:
    print(f"Other operating costs amounted to {format_currency_eur(abs(operating_costs))}.")

financial_result = get_safe_value(income_df, 'Financiële baten en lasten', current_year)
if financial_result != 0:
    if financial_result > 0:
        print(f"Financial income contributed {format_currency_eur(financial_result)} to the result.")
    else:
        print(f"Financial expenses reduced the result by {format_currency_eur(abs(financial_result))}.")

print("✅ Income statement analysis completed")