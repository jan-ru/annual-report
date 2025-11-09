"""
Enhanced Report Modules specifically designed for Telco B.V.
These modules replace the generic ones with Telco-specific formatting and calculations
"""

from pathlib import Path
from datetime import datetime

# ===== SETUP.PY MODULE =====
SETUP_MODULE = '''# Telco B.V. Report Setup Module
import pandas as pd
import json
import numpy as np
from pathlib import Path
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from great_tables import GT, md, html
from great_tables import loc, style

# Load processed data
data_dir = Path("processed_data")

# Load DataFrames
income_df = pd.read_csv(data_dir / "income_statement.csv")
balance_df = pd.read_csv(data_dir / "balance_sheet.csv")

# Load calculations and metadata
with open(data_dir / "calculations.json") as f:
    calculations = json.load(f)

with open(data_dir / "metadata.json") as f:
    metadata = json.load(f)

with open(data_dir / "chart_data.json") as f:
    chart_data = json.load(f)

# Telco-specific helper functions
def format_currency_eur(amount):
    """Format amount as EUR currency"""
    if pd.isna(amount) or amount == 0:
        return "€ 0"
    elif amount > 0:
        return f"€ {amount:,.0f}"
    else:
        return f"€ ({abs(amount):,.0f})"

def get_safe_value(df, category, year_col, default=0):
    """Safely get value from dataframe"""
    try:
        mask = df['Categorie'] == category
        if mask.any() and year_col in df.columns:
            return float(df.loc[mask, year_col].iloc[0])
    except:
        pass
    return default

# Define current and previous years
current_year = metadata['report_year']
previous_year = str(int(current_year) - 1)

print(f"✅ Telco B.V. report data loaded for {current_year}")
print(f"📊 Income statement: {income_df.shape[0]} categories")
print(f"📊 Balance sheet: {balance_df.shape[0]} categories")
'''

# ===== EXECUTIVE_SUMMARY.PY MODULE =====
EXECUTIVE_SUMMARY_MODULE = '''# Telco B.V. Executive Summary Module

print("## Telco B.V. - Financial Highlights")
print()

company_name = metadata.get('company_name', 'Telco B.V.')
report_date = f"December 31, {current_year}"

# Extract key financial metrics
revenue_current = get_safe_value(income_df, 'Netto-omzet', current_year)
revenue_previous = get_safe_value(income_df, 'Netto-omzet', previous_year)

operating_result_current = get_safe_value(income_df, 'Bedrijfsresultaat', current_year)
net_result_current = get_safe_value(income_df, 'Resultaat na belastingen', current_year)

total_assets_current = get_safe_value(balance_df, 'Totaal activa', current_year)
equity_current = get_safe_value(balance_df, 'Eigen vermogen', current_year)

# Calculate key ratios
equity_ratio = (equity_current / total_assets_current * 100) if total_assets_current > 0 else 0

print(f"**Company:** {company_name}")
print(f"**Reporting Period:** Year ended {report_date}")
print()

print("### Financial Performance")
print(f"- **Revenue:** {format_currency_eur(revenue_current)} ({current_year}) vs {format_currency_eur(revenue_previous)} ({previous_year})")

if revenue_current > revenue_previous and revenue_previous > 0:
    growth = ((revenue_current - revenue_previous) / revenue_previous * 100)
    print(f"- **Revenue Growth:** {growth:.1f}%")
elif revenue_current > 0 and revenue_previous == 0:
    print(f"- **Revenue Growth:** New revenue stream established")
else:
    print(f"- **Revenue Change:** Decreased from previous year")

print(f"- **Operating Result:** {format_currency_eur(operating_result_current)}")
print(f"- **Net Result:** {format_currency_eur(net_result_current)}")
print()

print("### Financial Position")
print(f"- **Total Assets:** {format_currency_eur(total_assets_current)}")
print(f"- **Equity:** {format_currency_eur(equity_current)}")
print(f"- **Equity Ratio:** {equity_ratio:.1f}%")
print()

print("### Key Developments in 2024")
real_estate_inv = get_safe_value(balance_df, 'Vastgoedbeleggingen', current_year)
if real_estate_inv > 0:
    print(f"- Investment in real estate: {format_currency_eur(real_estate_inv)}")

fin_assets_current = get_safe_value(balance_df, 'Financiële vaste activa', current_year)
fin_assets_previous = get_safe_value(balance_df, 'Financiële vaste activa', previous_year)
if fin_assets_current < fin_assets_previous:
    decrease = fin_assets_previous - fin_assets_current
    print(f"- Reduction in financial assets: {format_currency_eur(decrease)}")

print("- Continued focus on investment activities and financial management")
'''

# ===== INCOME_STATEMENT.PY MODULE =====
INCOME_STATEMENT_MODULE = '''# Telco B.V. Income Statement Module

print("## Income Statement")
print()

# Create Great Tables version of income statement
income_display_df = income_df.copy()

# Ensure we have the right columns
year_columns = [col for col in income_display_df.columns if col.isdigit()]
if not year_columns:
    # Handle alternative column naming
    year_columns = [col for col in income_display_df.columns if current_year in col or previous_year in col]

# Create the Great Table
income_gt = (
    GT(income_display_df)
    .tab_header(
        title="Income Statement (Winst-en-verliesrekening)",
        subtitle=f"Years ending December 31, {current_year} and {previous_year} (in EUR)"
    )
    .cols_label(
        Categorie="",
        Note="Note"
    )
)

# Format currency columns
for col in year_columns:
    if col in income_display_df.columns:
        income_gt = income_gt.fmt_currency(
            columns=[col],
            currency="EUR",
            placement="left",
            decimals=0
        )

# Style revenue row
revenue_mask = income_display_df['Categorie'] == 'Netto-omzet'
if revenue_mask.any():
    revenue_indices = income_display_df.index[revenue_mask].tolist()
    income_gt = income_gt.tab_style(
        style=style.fill(color="#e6f3ff") + style.text(weight="bold"),
        locations=loc.body(rows=revenue_indices)
    )

# Style result rows
result_categories = ['Bedrijfsresultaat', 'Resultaat voor belastingen', 'Resultaat na belastingen']
for category in result_categories:
    category_mask = income_display_df['Categorie'] == category
    if category_mask.any():
        indices = income_display_df.index[category_mask].tolist()
        income_gt = income_gt.tab_style(
            style=style.fill(color="#f0f8e6") + style.text(weight="bold"),
            locations=loc.body(rows=indices)
        )

# Add borders for better readability
income_gt = income_gt.tab_style(
    style=style.borders(sides="top", weight="2px"),
    locations=loc.body(rows=[0])
)

income_gt.show()

# Analysis
print()
print("### Income Statement Analysis")

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
'''

# ===== BALANCE_SHEET.PY MODULE =====
BALANCE_SHEET_MODULE = '''# Telco B.V. Balance Sheet Module

print("## Balance Sheet")
print()

# Create Great Tables version of balance sheet
balance_display_df = balance_df.copy()

# Ensure we have the right columns
year_columns = [col for col in balance_display_df.columns if col.isdigit()]
if not year_columns:
    year_columns = [col for col in balance_display_df.columns if current_year in col or previous_year in col]

# Create the Great Table
balance_gt = (
    GT(balance_display_df)
    .tab_header(
        title="Balance Sheet (Balans)",
        subtitle=f"As of December 31, {current_year} and {previous_year} (in EUR)"
    )
    .cols_label(
        Categorie="",
        Note="Note"
    )
)

# Format currency columns
for col in year_columns:
    if col in balance_display_df.columns:
        balance_gt = balance_gt.fmt_currency(
            columns=[col],
            currency="EUR",
            placement="left",
            decimals=0
        )

# Style asset totals
asset_categories = ['Totaal activa']
for category in asset_categories:
    category_mask = balance_display_df['Categorie'] == category
    if category_mask.any():
        indices = balance_display_df.index[category_mask].tolist()
        balance_gt = balance_gt.tab_style(
            style=style.fill(color="#e6f3ff") + style.text(weight="bold") + style.borders(sides="top", weight="2px"),
            locations=loc.body(rows=indices)
        )

# Style equity
equity_categories = ['Eigen vermogen', 'Totaal passiva']
for category in equity_categories:
    category_mask = balance_display_df['Categorie'] == category
    if category_mask.any():
        indices = balance_display_df.index[category_mask].tolist()
        balance_gt = balance_gt.tab_style(
            style=style.fill(color="#f0f8e6") + style.text(weight="bold"),
            locations=loc.body(rows=indices)
        )

balance_gt.show()

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

equity_ratio = (equity_current / total_assets_current * 100) if total_assets_current > 0 else 0
print(f"The equity ratio is {equity_ratio:.1f}%, indicating a strong financial position.")
'''

# ===== FINANCIAL_RATIOS.PY MODULE =====
FINANCIAL_RATIOS_MODULE = '''# Telco B.V. Financial Ratios Module

print("## Financial Ratios and Key Performance Indicators")
print()

# Calculate and display key financial ratios
def calculate_telco_ratios():
    ratios = {}
    
    # Profitability ratios
    revenue = get_safe_value(income_df, 'Netto-omzet', current_year)
    operating_result = get_safe_value(income_df, 'Bedrijfsresultaat', current_year)
    net_result = get_safe_value(income_df, 'Resultaat na belastingen', current_year)
    
    if revenue > 0:
        ratios['Operating_Margin'] = (operating_result / revenue) * 100
        ratios['Net_Margin'] = (net_result / revenue) * 100
    
    # Financial position ratios
    total_assets = get_safe_value(balance_df, 'Totaal activa', current_year)
    equity = get_safe_value(balance_df, 'Eigen vermogen', current_year)
    current_liabilities = get_safe_value(balance_df, 'Kortlopende schulden', current_year)
    
    if total_assets > 0:
        ratios['Equity_Ratio'] = (equity / total_assets) * 100
        ratios['Asset_Turnover'] = revenue / total_assets if revenue > 0 else 0
    
    # Liquidity ratios
    cash = get_safe_value(balance_df, 'Liquide middelen', current_year)
    receivables = get_safe_value(balance_df, 'Vorderingen', current_year)
    
    ratios['Working_Capital'] = cash + receivables - current_liabilities
    
    return ratios

ratios = calculate_telco_ratios()

print("### Key Performance Indicators")

if 'Equity_Ratio' in ratios:
    print(f"- **Equity Ratio:** {ratios['Equity_Ratio']:.1f}%")

if 'Working_Capital' in ratios:
    print(f"- **Working Capital:** {format_currency_eur(ratios['Working_Capital'])}")

if 'Operating_Margin' in ratios and not np.isnan(ratios['Operating_Margin']):
    print(f"- **Operating Margin:** {ratios['Operating_Margin']:.1f}%")

if 'Asset_Turnover' in ratios:
    print(f"- **Asset Turnover:** {ratios['Asset_Turnover']:.2f}x")

print()
print("### Financial Strength Assessment")

equity_ratio = ratios.get('Equity_Ratio', 0)
if equity_ratio > 50:
    print("- **Equity Position:** Strong - equity ratio above 50%")
elif equity_ratio > 30:
    print("- **Equity Position:** Adequate - equity ratio between 30-50%")
else:
    print("- **Equity Position:** Needs attention - equity ratio below 30%")

working_capital = ratios.get('Working_Capital', 0)
if working_capital > 100000:
    print("- **Liquidity:** Strong - sufficient working capital")
elif working_capital > 0:
    print("- **Liquidity:** Adequate - positive working capital")
else:
    print("- **Liquidity:** Tight - negative working capital")
'''

# ===== REVENUE_CHARTS.PY MODULE =====
REVENUE_CHARTS_MODULE = '''# Telco B.V. Revenue Charts Module

print("## Revenue Analysis")
print()

# Extract revenue data
revenue_current = get_safe_value(income_df, 'Netto-omzet', current_year)
revenue_previous = get_safe_value(income_df, 'Netto-omzet', previous_year)

if revenue_current > 0 or revenue_previous > 0:
    # Revenue trend chart
    fig = go.Figure()
    
    years = [previous_year, current_year]
    revenues = [revenue_previous, revenue_current]
    
    fig.add_trace(go.Bar(
        x=years,
        y=revenues,
        name='Revenue',
        marker_color=['#1f77b4', '#ff7f0e'],
        text=[format_currency_eur(rev) for rev in revenues],
        textposition='auto',
        textfont=dict(size=12, color='white')
    ))
    
    fig.update_layout(
        title="Revenue Development",
        xaxis_title="Year",
        yaxis_title="Revenue (EUR)",
        showlegend=False,
        height=400,
        plot_bgcolor='white',
        yaxis=dict(gridcolor='lightgray')
    )
    
    fig.show()
    
    # Revenue growth analysis
    if revenue_previous > 0 and revenue_current > revenue_previous:
        growth_rate = ((revenue_current - revenue_previous) / revenue_previous) * 100
        print(f"Revenue grew by {growth_rate:.1f}% from {previous_year} to {current_year}.")
    elif revenue_previous == 0 and revenue_current > 0:
        print(f"New revenue streams established in {current_year}.")
    elif revenue_current < revenue_previous:
        decline_rate = ((revenue_previous - revenue_current) / revenue_previous) * 100
        print(f"Revenue decreased by {decline_rate:.1f}% from {previous_year} to {current_year}.")
    
else:
    print("No significant revenue to analyze for the current period.")
'''

# ===== ASSET_CHARTS.PY MODULE =====
ASSET_CHARTS_MODULE = '''# Telco B.V. Asset Charts Module

print("## Asset Composition Analysis")
print()

# Extract asset data
real_estate = get_safe_value(balance_df, 'Vastgoedbeleggingen', current_year)
financial_assets = get_safe_value(balance_df, 'Financiële vaste activa', current_year)
receivables = get_safe_value(balance_df, 'Vorderingen', current_year)
securities = get_safe_value(balance_df, 'Effecten', current_year)
cash = get_safe_value(balance_df, 'Liquide middelen', current_year)

asset_categories = ['Real Estate Investments', 'Financial Assets', 'Receivables', 'Securities', 'Cash']
asset_values = [real_estate, financial_assets, receivables, securities, cash]

# Filter out zero values for the pie chart
non_zero_assets = [(cat, val) for cat, val in zip(asset_categories, asset_values) if val > 0]

if non_zero_assets:
    categories, values = zip(*non_zero_assets)
    
    # Asset composition pie chart
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    fig = go.Figure(data=[go.Pie(
        labels=categories,
        values=values,
        hole=0.4,
        marker_colors=colors[:len(categories)],
        textinfo='label+percent',
        textfont_size=10
    )])
    
    fig.update_layout(
        title=f"Asset Composition - {current_year}",
        height=500,
        showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5)
    )
    
    fig.show()
    
    # Asset analysis
    total_assets = sum(values)
    print(f"### Asset Distribution Analysis")
    print()
    
    for category, value in zip(categories, values):
        percentage = (value / total_assets) * 100
        print(f"- **{category}:** {format_currency_eur(value)} ({percentage:.1f}%)")
    
    print()
    if real_estate > 0:
        real_estate_pct = (real_estate / total_assets) * 100
        print(f"Real estate investments represent {real_estate_pct:.1f}% of total assets, ")
        print("indicating the company's strategic focus on property investments.")
    
    if financial_assets > 0:
        fin_assets_pct = (financial_assets / total_assets) * 100
        print(f"Financial assets constitute {fin_assets_pct:.1f}% of the asset base, ")
        print("reflecting the company's investment and lending activities.")

else:
    print("No significant asset composition to analyze.")
'''

# ===== NOTES.PY MODULE =====
NOTES_MODULE = '''# Telco B.V. Notes and Methodology Module

print("## Notes to the Financial Statements")
print()

print("### Basis of Preparation")
print("- These financial statements have been prepared in accordance with Dutch GAAP (Title 9, Book 2 of the Dutch Civil Code)")
print("- Figures are presented in Euros (EUR)")  
print("- The company has elected to apply fiscal valuation principles as permitted under Dutch law")
print()

print("### Principal Activities")
print("Telco B.V.'s main activities include:")
print("- Establishing and participating in other companies and enterprises")
print("- Providing management, consultancy, and interim management services")
print("- Managing and exploiting movable and immovable property")
print("- Investment activities in securities and other financial instruments")
print("- Pension provision services for directors and related parties")
print()

print("### Significant Accounting Policies")
print("**Real Estate Investments:** Valued at cost less accumulated depreciation")
print("**Financial Fixed Assets:** Participations valued using equity method; other investments at cost")
print("**Receivables:** Stated at nominal value less provisions for doubtful debts")
print("**Securities:** Valued at market value where available, otherwise at cost")
print()

print("### Key Estimates and Judgments")
print("- Valuation of unlisted investments requires management judgment")
print("- Assessment of recoverability of loans and receivables")
print("- Determination of useful lives for depreciation purposes")
print()

print("### Subsequent Events")
print("No significant events occurred between the balance sheet date and the date of ")
print("preparation of these financial statements that would require adjustment or disclosure.")
print()

print("### Report Generation Information")
print(f"- **Generated on:** {metadata.get('generation_date', 'Unknown')}")
print(f"- **System:** Automated Financial Reporting System v2.0 (Modular)")
print(f"- **Data Source:** {'XAF processing' if 'xaf' in metadata else 'Sample data'}")
print("- **Quality Assurance:** Automated validation and business rule checking applied")
'''

def create_telco_modules(output_dir: Path):
    """Create all Telco-specific modules"""
    
    modules_dir = output_dir / "report_modules"
    modules_dir.mkdir(exist_ok=True)
    
    modules = {
        "setup.py": SETUP_MODULE,
        "executive_summary.py": EXECUTIVE_SUMMARY_MODULE,
        "income_statement.py": INCOME_STATEMENT_MODULE,
        "balance_sheet.py": BALANCE_SHEET_MODULE,
        "financial_ratios.py": FINANCIAL_RATIOS_MODULE,
        "revenue_charts.py": REVENUE_CHARTS_MODULE,
        "asset_charts.py": ASSET_CHARTS_MODULE,
        "notes.py": NOTES_MODULE
    }
    
    print("🔨 Creating Telco-specific report modules...")
    
    for filename, content in modules.items():
        module_file = modules_dir / filename
        with open(module_file, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✅ Created: {filename}")
    
    print(f"📁 All modules created in: {modules_dir}")
    return modules_dir


def create_telco_qmd_template(output_dir: Path, company_name: str, report_year: str) -> Path:
    """Create the main QMD template for Telco B.V."""
    
    qmd_content = f'''---
title: "{company_name} - Jaarrekening {report_year}"
subtitle: "Annual Financial Statements"
author: "Automated Financial Reporting System"
date: "{datetime.now().strftime('%B %d, %Y')}"
format:
  html:
    theme: cosmo
    toc: true
    toc-depth: 3
    number-sections: true
    code-fold: true
    fig-width: 10
    fig-height: 6
    css: |
      .gt_table {{
        font-family: "Arial", sans-serif;
        font-size: 12px;
      }}
      .executive-summary {{
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 8px;
        margin: 20px 0;
      }}
  pdf:
    documentclass: article
    geometry: 
      - margin=1in
      - a4paper
    fig-width: 8
    fig-height: 5
    fontsize: 11pt
execute:
  echo: false
  warning: false
  message: false
  error: false
jupyter: python3
---

```{{python}}
# Setup and data loading
exec(open('report_modules/setup.py').read())
```

::: {{.executive-summary}}

```{{python}}
# Executive Summary
exec(open('report_modules/executive_summary.py').read())
```

:::

# Jaarrekening {report_year}

## Winst-en-verliesrekening (Income Statement)

```{{python}}
# Income Statement
exec(open('report_modules/income_statement.py').read())
```

## Balans (Balance Sheet)

```{{python}}
# Balance Sheet  
exec(open('report_modules/balance_sheet.py').read())
```

# Financial Analysis

```{{python}}
# Financial Ratios and KPIs
exec(open('report_modules/financial_ratios.py').read())
```

# Charts and Visualizations

```{{python}}
# Revenue Analysis Charts
exec(open('report_modules/revenue_charts.py').read())
```

```{{python}}
# Asset Composition Charts
exec(open('report_modules/asset_charts.py').read())
```

# Toelichting (Notes)

```{{python}}
# Notes and Methodology
exec(open('report_modules/notes.py').read())
```

---

*Dit rapport is automatisch gegenereerd op {datetime.now().strftime('%d-%m-%Y om %H:%M')} door het Automated Financial Reporting System.*

*This report was automatically generated on {datetime.now().strftime('%B %d, %Y at %H:%M')} by the Automated Financial Reporting System.*
'''
    
    # Create the QMD file
    qmd_file = output_dir / f"{company_name.lower().replace(' ', '_').replace('.', '')}_jaarrekening_{report_year}.qmd"
    
    with open(qmd_file, 'w', encoding='utf-8') as f:
        f.write(qmd_content)
    
    print(f"📄 Created QMD template: {qmd_file}")
    return qmd_file


if __name__ == "__main__":
    # Test the module creation
    test_dir = Path("test_telco_modules")
    test_dir.mkdir(exist_ok=True)
    
    modules_dir = create_telco_modules(test_dir)
    qmd_file = create_telco_qmd_template(test_dir, "Telco B.V.", "2024")
    
    print(f"\n✅ Test completed!")
    print(f"📁 Modules: {modules_dir}")
    print(f"📄 Template: {qmd_file}")
    print(f"\n🔄 To test rendering:")
    print(f"cd {test_dir}")
    print(f"quarto render {qmd_file.name}")