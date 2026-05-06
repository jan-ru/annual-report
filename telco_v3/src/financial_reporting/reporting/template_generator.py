"""
Template Generator Module

Generates Quarto templates and Python report modules for Telco B.V. financial reports.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Report module content (written to disk, executed by Quarto via exec())
# ---------------------------------------------------------------------------

_SETUP_MODULE = '''# Telco B.V. Report Setup Module
import pandas as pd
import json
import numpy as np
from pathlib import Path
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from great_tables import GT, md, html
from great_tables import loc, style

data_dir = Path("processed_data")

income_df = pd.read_csv(data_dir / "income_statement.csv")
balance_df = pd.read_csv(data_dir / "balance_sheet.csv")

with open(data_dir / "calculations.json") as f:
    calculations = json.load(f)

with open(data_dir / "metadata.json") as f:
    metadata = json.load(f)

with open(data_dir / "chart_data.json") as f:
    chart_data = json.load(f)


def format_currency_eur(amount):
    """Format amount as EUR currency."""
    if pd.isna(amount) or amount == 0:
        return "€ 0"
    elif amount > 0:
        return f"€ {amount:,.0f}"
    else:
        return f"€ ({abs(amount):,.0f})"


def get_safe_value(df, category, year_col, default=0):
    """Safely get value from dataframe."""
    try:
        mask = df["Categorie"] == category
        if mask.any() and year_col in df.columns:
            return float(df.loc[mask, year_col].iloc[0])
    except Exception:
        pass
    return default


current_year = metadata["report_year"]
previous_year = str(int(current_year) - 1)

print(f"✅ Telco B.V. report data loaded for {current_year}")
print(f"📊 Income statement: {income_df.shape[0]} categories")
print(f"📊 Balance sheet: {balance_df.shape[0]} categories")
'''

_EXECUTIVE_SUMMARY_MODULE = '''# Telco B.V. Executive Summary Module

print("## Telco B.V. - Financial Highlights")
print()

company_name = metadata.get("company_name", "Telco B.V.")
report_date = f"December 31, {current_year}"

revenue_current = get_safe_value(income_df, "Netto-omzet", current_year)
revenue_previous = get_safe_value(income_df, "Netto-omzet", previous_year)
operating_result_current = get_safe_value(income_df, "Bedrijfsresultaat", current_year)
net_result_current = get_safe_value(income_df, "Resultaat na belastingen", current_year)
total_assets_current = get_safe_value(balance_df, "Totaal activa", current_year)
equity_current = get_safe_value(balance_df, "Eigen vermogen", current_year)
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
    print("- **Revenue Growth:** New revenue stream established")
else:
    print("- **Revenue Change:** Decreased from previous year")

print(f"- **Operating Result:** {format_currency_eur(operating_result_current)}")
print(f"- **Net Result:** {format_currency_eur(net_result_current)}")
print()
print("### Financial Position")
print(f"- **Total Assets:** {format_currency_eur(total_assets_current)}")
print(f"- **Equity:** {format_currency_eur(equity_current)}")
print(f"- **Equity Ratio:** {equity_ratio:.1f}%")
'''

_INCOME_STATEMENT_MODULE = '''# Telco B.V. Income Statement Module

print("## Income Statement")
print()

income_display_df = income_df.copy()
year_columns = [col for col in income_display_df.columns if col.isdigit()]
if not year_columns:
    year_columns = [col for col in income_display_df.columns
                    if current_year in col or previous_year in col]

income_gt = (
    GT(income_display_df)
    .tab_header(
        title="Income Statement (Winst-en-verliesrekening)",
        subtitle=f"Years ending December 31, {current_year} and {previous_year} (in EUR)"
    )
    .cols_label(Categorie="", Note="Note")
)

for col in year_columns:
    if col in income_display_df.columns:
        income_gt = income_gt.fmt_currency(columns=[col], currency="EUR", placement="left", decimals=0)

revenue_mask = income_display_df["Categorie"] == "Netto-omzet"
if revenue_mask.any():
    revenue_indices = income_display_df.index[revenue_mask].tolist()
    income_gt = income_gt.tab_style(
        style=style.fill(color="#e6f3ff") + style.text(weight="bold"),
        locations=loc.body(rows=revenue_indices)
    )

for category in ["Bedrijfsresultaat", "Resultaat voor belastingen", "Resultaat na belastingen"]:
    mask = income_display_df["Categorie"] == category
    if mask.any():
        income_gt = income_gt.tab_style(
            style=style.fill(color="#f0f8e6") + style.text(weight="bold"),
            locations=loc.body(rows=income_display_df.index[mask].tolist())
        )

income_gt.show()
'''

_BALANCE_SHEET_MODULE = '''# Telco B.V. Balance Sheet Module

print("## Balance Sheet")
print()

balance_display_df = balance_df.copy()
year_columns = [col for col in balance_display_df.columns if col.isdigit()]
if not year_columns:
    year_columns = [col for col in balance_display_df.columns
                    if current_year in col or previous_year in col]

balance_gt = (
    GT(balance_display_df)
    .tab_header(
        title="Balance Sheet (Balans)",
        subtitle=f"As of December 31, {current_year} and {previous_year} (in EUR)"
    )
    .cols_label(Categorie="", Note="Note")
)

for col in year_columns:
    if col in balance_display_df.columns:
        balance_gt = balance_gt.fmt_currency(columns=[col], currency="EUR", placement="left", decimals=0)

for category in ["Totaal activa"]:
    mask = balance_display_df["Categorie"] == category
    if mask.any():
        balance_gt = balance_gt.tab_style(
            style=style.fill(color="#e6f3ff") + style.text(weight="bold") + style.borders(sides="top", weight="2px"),
            locations=loc.body(rows=balance_display_df.index[mask].tolist())
        )

for category in ["Eigen vermogen", "Totaal passiva"]:
    mask = balance_display_df["Categorie"] == category
    if mask.any():
        balance_gt = balance_gt.tab_style(
            style=style.fill(color="#f0f8e6") + style.text(weight="bold"),
            locations=loc.body(rows=balance_display_df.index[mask].tolist())
        )

balance_gt.show()
'''

_FINANCIAL_RATIOS_MODULE = '''# Telco B.V. Financial Ratios Module

print("## Financial Ratios and Key Performance Indicators")
print()


def calculate_telco_ratios():
    ratios = {}
    revenue = get_safe_value(income_df, "Netto-omzet", current_year)
    operating_result = get_safe_value(income_df, "Bedrijfsresultaat", current_year)
    net_result = get_safe_value(income_df, "Resultaat na belastingen", current_year)
    if revenue > 0:
        ratios["Operating_Margin"] = (operating_result / revenue) * 100
        ratios["Net_Margin"] = (net_result / revenue) * 100
    total_assets = get_safe_value(balance_df, "Totaal activa", current_year)
    equity = get_safe_value(balance_df, "Eigen vermogen", current_year)
    current_liabilities = get_safe_value(balance_df, "Kortlopende schulden", current_year)
    if total_assets > 0:
        ratios["Equity_Ratio"] = (equity / total_assets) * 100
        ratios["Asset_Turnover"] = revenue / total_assets if revenue > 0 else 0
    cash = get_safe_value(balance_df, "Liquide middelen", current_year)
    receivables = get_safe_value(balance_df, "Vorderingen", current_year)
    ratios["Working_Capital"] = cash + receivables - current_liabilities
    return ratios


ratios = calculate_telco_ratios()

print("### Key Performance Indicators")
if "Equity_Ratio" in ratios:
    print(f"- **Equity Ratio:** {ratios['Equity_Ratio']:.1f}%")
if "Working_Capital" in ratios:
    print(f"- **Working Capital:** {format_currency_eur(ratios['Working_Capital'])}")
if "Operating_Margin" in ratios and not np.isnan(ratios["Operating_Margin"]):
    print(f"- **Operating Margin:** {ratios['Operating_Margin']:.1f}%")
if "Asset_Turnover" in ratios:
    print(f"- **Asset Turnover:** {ratios['Asset_Turnover']:.2f}x")
'''

_REVENUE_CHARTS_MODULE = '''# Telco B.V. Revenue Charts Module

print("## Revenue Analysis")
print()

revenue_current = get_safe_value(income_df, "Netto-omzet", current_year)
revenue_previous = get_safe_value(income_df, "Netto-omzet", previous_year)

if revenue_current > 0 or revenue_previous > 0:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[previous_year, current_year],
        y=[revenue_previous, revenue_current],
        name="Revenue",
        marker_color=["#1f77b4", "#ff7f0e"],
        text=[format_currency_eur(rev) for rev in [revenue_previous, revenue_current]],
        textposition="auto",
        textfont=dict(size=12, color="white")
    ))
    fig.update_layout(
        title="Revenue Development",
        xaxis_title="Year",
        yaxis_title="Revenue (EUR)",
        showlegend=False,
        height=400,
        plot_bgcolor="white",
        yaxis=dict(gridcolor="lightgray")
    )
    fig.show()
else:
    print("No significant revenue to analyze for the current period.")
'''

_ASSET_CHARTS_MODULE = '''# Telco B.V. Asset Charts Module

print("## Asset Composition Analysis")
print()

real_estate = get_safe_value(balance_df, "Vastgoedbeleggingen", current_year)
financial_assets = get_safe_value(balance_df, "Financiële vaste activa", current_year)
receivables = get_safe_value(balance_df, "Vorderingen", current_year)
securities = get_safe_value(balance_df, "Effecten", current_year)
cash = get_safe_value(balance_df, "Liquide middelen", current_year)

asset_categories = ["Real Estate Investments", "Financial Assets", "Receivables", "Securities", "Cash"]
asset_values = [real_estate, financial_assets, receivables, securities, cash]
non_zero_assets = [(cat, val) for cat, val in zip(asset_categories, asset_values) if val > 0]

if non_zero_assets:
    categories, values = zip(*non_zero_assets)
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
    fig = go.Figure(data=[go.Pie(
        labels=categories,
        values=values,
        hole=0.4,
        marker_colors=colors[:len(categories)],
        textinfo="label+percent",
        textfont_size=10
    )])
    fig.update_layout(
        title=f"Asset Composition - {current_year}",
        height=500,
        showlegend=True
    )
    fig.show()
else:
    print("No significant asset composition to analyze.")
'''

_NOTES_MODULE = '''# Telco B.V. Notes and Methodology Module

print("## Notes to the Financial Statements")
print()
print("### Basis of Preparation")
print("- Prepared in accordance with Dutch GAAP (Title 9, Book 2 of the Dutch Civil Code)")
print("- Figures presented in Euros (EUR)")
print("- Fiscal valuation principles applied as permitted under Dutch law")
print()
print("### Principal Activities")
print("- Participating in other companies and enterprises")
print("- Management, consultancy, and interim management services")
print("- Managing and exploiting movable and immovable property")
print("- Investment activities in securities and other financial instruments")
print()
print("### Significant Accounting Policies")
print("**Real Estate:** Valued at cost less accumulated depreciation")
print("**Financial Fixed Assets:** Equity method for participations; cost for other investments")
print("**Receivables:** Nominal value less provisions for doubtful debts")
print("**Securities:** Market value where available, otherwise at cost")
print()
print("### Report Generation Information")
print(f"- **Generated on:** {metadata.get('generation_date', 'Unknown')}")
print(f"- **System:** Automated Financial Reporting System (Modular)")
'''

# ---------------------------------------------------------------------------
# Module creation helpers
# ---------------------------------------------------------------------------

_MODULE_FILES = {
    "data_loader.py": _SETUP_MODULE,
    "executive_summary.py": _EXECUTIVE_SUMMARY_MODULE,
    "income_statement.py": _INCOME_STATEMENT_MODULE,
    "balance_sheet.py": _BALANCE_SHEET_MODULE,
    "financial_ratios.py": _FINANCIAL_RATIOS_MODULE,
    "revenue_charts.py": _REVENUE_CHARTS_MODULE,
    "asset_charts.py": _ASSET_CHARTS_MODULE,
    "notes.py": _NOTES_MODULE,
}


def create_telco_modules(output_dir: Path) -> Path:
    """Write all Telco-specific report modules to *output_dir/report_modules/*."""
    modules_dir = output_dir / "report_modules"
    modules_dir.mkdir(exist_ok=True)

    print("🔨 Creating Telco-specific report modules...")
    for filename, content in _MODULE_FILES.items():
        (modules_dir / filename).write_text(content, encoding="utf-8")
        print(f"  ✅ Created: {filename}")

    print(f"📁 All modules created in: {modules_dir}")
    return modules_dir


def create_telco_qmd_template(
    output_dir: Path,
    company_name: str,
    report_year: str,
) -> Path:
    """Create the main Quarto (QMD) template for Telco B.V."""
    now = datetime.now()
    qmd_content = f'''---
title: "{company_name} - Jaarrekening {report_year}"
subtitle: "Annual Financial Statements"
author: "Automated Financial Reporting System"
date: "{now.strftime('%B %d, %Y')}"
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
exec(open('report_modules/data_loader.py').read())
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
exec(open('report_modules/income_statement.py').read())
```

## Balans (Balance Sheet)

```{{python}}
exec(open('report_modules/balance_sheet.py').read())
```

# Financial Analysis

```{{python}}
exec(open('report_modules/financial_ratios.py').read())
```

# Charts and Visualizations

```{{python}}
exec(open('report_modules/revenue_charts.py').read())
```

```{{python}}
exec(open('report_modules/asset_charts.py').read())
```

# Toelichting (Notes)

```{{python}}
exec(open('report_modules/notes.py').read())
```

---

*Dit rapport is automatisch gegenereerd op {now.strftime('%d-%m-%Y om %H:%M')} door het Automated Financial Reporting System.*

*This report was automatically generated on {now.strftime('%B %d, %Y at %H:%M')} by the Automated Financial Reporting System.*
'''

    safe_name = company_name.lower().replace(" ", "_").replace(".", "")
    qmd_file = output_dir / f"{safe_name}_jaarrekening_{report_year}.qmd"
    qmd_file.write_text(qmd_content, encoding="utf-8")
    print(f"📄 Created QMD template: {qmd_file}")
    return qmd_file


# ---------------------------------------------------------------------------
# ReportTemplateGenerator — thin wrapper class
# ---------------------------------------------------------------------------


class ReportTemplateGenerator:
    """Generates Quarto templates with minimal inline Python code blocks."""

    def __init__(self, company_name: str, report_year: str) -> None:
        self.company_name = company_name
        self.report_year = report_year

    def generate_main_template(self, output_dir: Path) -> Path:
        return create_telco_qmd_template(output_dir, self.company_name, self.report_year)

    def generate_python_modules(self, output_dir: Path) -> None:
        create_telco_modules(output_dir)


__all__ = [
    "ReportTemplateGenerator",
    "create_telco_modules",
    "create_telco_qmd_template",
]
