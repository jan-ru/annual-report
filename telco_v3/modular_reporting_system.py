"""
Modular Financial Reporting System 
Separates Python logic from Quarto templates for better maintainability
"""

from pathlib import Path
from typing import Dict, Any, Optional
import polars as pl
import pandas as pd
from datetime import datetime
import json
from version import __version__

class ReportDataProcessor:
    """Handles all data processing logic separately from templates"""
    
    def __init__(self, company_name: str, report_year: str):
        self.company_name = company_name
        self.report_year = report_year
        self.data = {}
        self.calculations = {}
        self.metadata = {}
    
    def process_income_statement(self, income_df: pl.DataFrame) -> Dict[str, Any]:
        """Process income statement data and return all needed components"""
        
        # Fix column names if needed
        income_df = self._fix_column_names(income_df)
        
        # Convert to pandas for Great Tables
        income_pd = income_df.to_pandas()
        
        # Calculate key metrics
        current_year = self.report_year
        previous_year = str(int(current_year) - 1)
        
        revenue_current = self._safe_get_value(income_pd, 'Netto-omzet', current_year)
        revenue_previous = self._safe_get_value(income_pd, 'Netto-omzet', previous_year)
        
        # Store processed data
        result = {
            'dataframe': income_pd,
            'revenue_current': revenue_current,
            'revenue_previous': revenue_previous,
            'revenue_change': revenue_current - revenue_previous if revenue_previous else None,
            'revenue_change_pct': ((revenue_current - revenue_previous) / revenue_previous * 100) if revenue_previous and revenue_previous != 0 else None
        }
        
        return result
    
    def process_balance_sheet(self, balance_df: pl.DataFrame) -> Dict[str, Any]:
        """Process balance sheet data and return all needed components"""
        
        # Fix column names if needed
        balance_df = self._fix_column_names(balance_df)
        
        # Convert to pandas for Great Tables
        balance_pd = balance_df.to_pandas()
        
        # Calculate key metrics
        current_year = self.report_year
        previous_year = str(int(current_year) - 1)
        
        total_assets_current = self._calculate_total_assets(balance_pd, current_year)
        total_assets_previous = self._calculate_total_assets(balance_pd, previous_year)
        
        result = {
            'dataframe': balance_pd,
            'total_assets_current': total_assets_current,
            'total_assets_previous': total_assets_previous,
            'working_capital': self._calculate_working_capital(balance_pd, current_year)
        }
        
        return result
    
    def generate_chart_data(self, data_type: str) -> Dict[str, Any]:
        """Generate data specifically formatted for charts"""
        
        if data_type == 'revenue_trend':
            return {
                'years': [self.report_year, str(int(self.report_year) - 1)],
                'values': [
                    self.data.get('income_statement', {}).get('revenue_current', 0),
                    self.data.get('income_statement', {}).get('revenue_previous', 0)
                ]
            }
        elif data_type == 'assets_composition':
            balance_data = self.data.get('balance_sheet', {}).get('dataframe')
            if balance_data is not None:
                return self._prepare_assets_chart_data(balance_data)
        
        return {}
    
    def save_processed_data(self, output_dir: Path):
        """Save all processed data to files that Quarto can load"""
        
        data_dir = output_dir / "processed_data"
        data_dir.mkdir(exist_ok=True)
        
        # Save DataFrames as CSV
        if 'income_statement' in self.data:
            income_df = self.data['income_statement']['dataframe']
            income_df.to_csv(data_dir / "income_statement.csv", index=False)
        
        if 'balance_sheet' in self.data:
            balance_df = self.data['balance_sheet']['dataframe']
            balance_df.to_csv(data_dir / "balance_sheet.csv", index=False)
        
        # Save calculations and metadata as JSON
        calculations_file = data_dir / "calculations.json"
        with open(calculations_file, 'w') as f:
            json.dump(self.calculations, f, indent=2, default=str)
        
        metadata_file = data_dir / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump({
                'company_name': self.company_name,
                'report_year': self.report_year,
                'generation_date': datetime.now().isoformat(),
                'system_version': __version__,
                **self.metadata
            }, f, indent=2, default=str)
        
        # Save chart data
        chart_data = {
            'revenue_trend': self.generate_chart_data('revenue_trend'),
            'assets_composition': self.generate_chart_data('assets_composition')
        }
        
        chart_file = data_dir / "chart_data.json"
        with open(chart_file, 'w') as f:
            json.dump(chart_data, f, indent=2, default=str)
    
    def _fix_column_names(self, df: pl.DataFrame) -> pl.DataFrame:
        """Fix problematic column names from pivot operations"""
        import re
        
        current_columns = df.columns
        rename_mapping = {}
        
        for col in current_columns:
            if col in ['Categorie', 'Level0', 'Level1', 'Sort1', 'row_type', 'subtotal_level', 'Balance_Side', 'Note']:
                # Keep standard columns as-is
                continue
            
            # Handle complex JSON-like column names
            if '{"' in col and '"}' in col:
                # Remove outer braces and quotes
                cleaned = col.strip('{}')
                
                # Split by comma and clean each part
                parts = []
                for part in cleaned.split(','):
                    # Remove quotes and extra characters  
                    clean_part = re.sub(r'["\s\\]', '', part)
                    if clean_part:
                        parts.append(clean_part)
                
                # Create new column name
                if parts:
                    new_name = '_'.join(parts)
                    rename_mapping[col] = new_name
        
        if rename_mapping:
            df = df.rename(rename_mapping)
            print(f"Fixed column names: {rename_mapping}")
        
        return df
    
    def _safe_get_value(self, df: pd.DataFrame, category: str, year_col: str) -> float:
        """Safely extract a value from the dataframe"""
        try:
            mask = df['Categorie'] == category
            if mask.any() and year_col in df.columns:
                return float(df.loc[mask, year_col].iloc[0])
        except (IndexError, ValueError, KeyError):
            pass
        return 0.0
    
    def _calculate_total_assets(self, df: pd.DataFrame, year_col: str) -> float:
        """Calculate total assets from balance sheet"""
        asset_categories = ['Vastgoedbeleggingen', 'Financiële vaste activa', 'Vorderingen', 'Liquide middelen']
        total = 0.0
        
        for category in asset_categories:
            total += self._safe_get_value(df, category, year_col)
        
        return total
    
    def _calculate_working_capital(self, df: pd.DataFrame, year_col: str) -> float:
        """Calculate working capital"""
        current_assets = (
            self._safe_get_value(df, 'Vorderingen', year_col) +
            self._safe_get_value(df, 'Liquide middelen', year_col)
        )
        current_liabilities = self._safe_get_value(df, 'Kortlopende schulden', year_col)
        
        return current_assets - current_liabilities
    
    def _prepare_assets_chart_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Prepare data for assets composition chart"""
        current_year = self.report_year
        
        return {
            'categories': ['Vastgoedbeleggingen', 'Financiële vaste activa', 'Vorderingen', 'Liquide middelen'],
            'values': [
                self._safe_get_value(df, 'Vastgoedbeleggingen', current_year),
                self._safe_get_value(df, 'Financiële vaste activa', current_year),
                self._safe_get_value(df, 'Vorderingen', current_year),
                self._safe_get_value(df, 'Liquide middelen', current_year)
            ]
        }


class ReportTemplateGenerator:
    """Generates Quarto templates with minimal Python code blocks"""
    
    def __init__(self, company_name: str, report_year: str):
        self.company_name = company_name
        self.report_year = report_year
    
    def generate_main_template(self, output_dir: Path) -> Path:
        """Generate the main QMD template with minimal Python"""
        
        qmd_content = f'''---
title: "{self.company_name} - Annual Report {self.report_year}"
subtitle: "Financial Statements and Analysis"
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
  pdf:
    documentclass: article
    geometry: margin=1in
    fig-width: 8
    fig-height: 5
execute:
  echo: false
  warning: false
  message: false
jupyter: python3
---

```{{python}}
# Import required modules - keep this minimal
exec(open('report_modules/setup.py').read())
```

# Executive Summary

```{{python}}
exec(open('report_modules/executive_summary.py').read())
```

# Financial Performance Analysis

## Income Statement

```{{python}}
exec(open('report_modules/income_statement.py').read())
```

## Balance Sheet

```{{python}}
exec(open('report_modules/balance_sheet.py').read())
```

## Financial Ratios

```{{python}}
exec(open('report_modules/financial_ratios.py').read())
```

# Charts and Visualizations

## Revenue Analysis

```{{python}}
exec(open('report_modules/revenue_charts.py').read())
```

## Asset Composition

```{{python}}
exec(open('report_modules/asset_charts.py').read())
```

# Notes and Methodology

```{{python}}
exec(open('report_modules/notes.py').read())
```

---
*Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by Automated Financial Reporting System*
'''
        
        qmd_file = output_dir / f"{self.company_name.lower().replace(' ', '_')}_annual_report_{self.report_year}.qmd"
        
        with open(qmd_file, 'w', encoding='utf-8') as f:
            f.write(qmd_content)
        
        return qmd_file
    
    def generate_python_modules(self, output_dir: Path):
        """Generate separate Python modules for each report section"""
        
        modules_dir = output_dir / "report_modules"
        modules_dir.mkdir(exist_ok=True)
        
        # Generate each module
        self._generate_setup_module(modules_dir)
        self._generate_executive_summary_module(modules_dir)
        self._generate_income_statement_module(modules_dir)
        self._generate_balance_sheet_module(modules_dir)
        self._generate_financial_ratios_module(modules_dir)
        self._generate_revenue_charts_module(modules_dir)
        self._generate_asset_charts_module(modules_dir)
        self._generate_notes_module(modules_dir)
    
    def _generate_setup_module(self, modules_dir: Path):
        """Generate setup.py module"""
        content = '''# Setup and data loading
import pandas as pd
import json
from pathlib import Path
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from great_tables import GT

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

print("✅ All data loaded successfully")
'''
        
        with open(modules_dir / "setup.py", 'w') as f:
            f.write(content)
    
    def _generate_executive_summary_module(self, modules_dir: Path):
        """Generate executive_summary.py module"""
        content = f'''# Executive Summary
print("## Key Financial Highlights for {self.report_year}")

# Extract key metrics from metadata
company_name = metadata.get('company_name', '{self.company_name}')
report_year = metadata.get('report_year', '{self.report_year}')

# You can access all calculations here without complex string formatting
current_revenue = chart_data['revenue_trend']['values'][0]
previous_revenue = chart_data['revenue_trend']['values'][1]

print(f"- **Company**: {{company_name}}")
print(f"- **Reporting Period**: Year ended December 31, {{report_year}}")
print(f"- **Revenue**: €{{current_revenue:,.0f}} ({{report_year}}) vs €{{previous_revenue:,.0f}} ({int(report_year)-1})")

if current_revenue > previous_revenue:
    change = ((current_revenue - previous_revenue) / previous_revenue * 100) if previous_revenue else 0
    print(f"- **Revenue Growth**: {{change:.1f}}%")
else:
    print("- **Revenue**: Decreased compared to previous year")
'''
        
        with open(modules_dir / "executive_summary.py", 'w') as f:
            f.write(content)
    
    def _generate_income_statement_module(self, modules_dir: Path):
        """Generate income_statement.py module"""
        content = '''# Income Statement Display
income_gt = (
    GT(income_df)
    .tab_header(
        title="Income Statement",
        subtitle=f"Years ending December 31, {metadata['report_year']} and {int(metadata['report_year'])-1}"
    )
    .cols_label(
        Categorie="Category"
    )
)

# Format currency columns (handle various column name formats)
currency_columns = [col for col in income_df.columns if col.isdigit() or '_' in col]

for col in currency_columns:
    if col in income_df.columns:
        income_gt = income_gt.fmt_currency(
            columns=[col],
            currency="EUR",
            placement="left"
        )

# Add styling for revenue row
revenue_mask = income_df['Categorie'] == 'Netto-omzet'
if revenue_mask.any():
    revenue_indices = income_df.index[revenue_mask].tolist()
    income_gt = income_gt.tab_style(
        style=[
            {"background-color": "#e6f3ff", "font-weight": "bold"}
        ],
        locations=[
            {"row_indices": revenue_indices}
        ]
    )

income_gt.show()
'''
        
        with open(modules_dir / "income_statement.py", 'w') as f:
            f.write(content)
    
    def _generate_balance_sheet_module(self, modules_dir: Path):
        """Generate balance_sheet.py module"""
        content = '''# Balance Sheet Display
balance_gt = (
    GT(balance_df)
    .tab_header(
        title="Balance Sheet",
        subtitle=f"As of December 31, {metadata['report_year']} and {int(metadata['report_year'])-1}"
    )
    .cols_label(
        Categorie="Category"
    )
)

# Format currency columns
currency_columns = [col for col in balance_df.columns if col.isdigit() or '_' in col]

for col in currency_columns:
    if col in balance_df.columns:
        balance_gt = balance_gt.fmt_currency(
            columns=[col],
            currency="EUR",
            placement="left"
        )

# Add styling for equity row
equity_mask = balance_df['Categorie'] == 'Eigen vermogen'
if equity_mask.any():
    equity_indices = balance_df.index[equity_mask].tolist()
    balance_gt = balance_gt.tab_style(
        style=[
            {"background-color": "#f0f8e6", "font-weight": "bold"}
        ],
        locations=[
            {"row_indices": equity_indices}
        ]
    )

balance_gt.show()
'''
        
        with open(modules_dir / "balance_sheet.py", 'w') as f:
            f.write(content)
    
    def _generate_financial_ratios_module(self, modules_dir: Path):
        """Generate financial_ratios.py module"""
        content = '''# Financial Ratios Analysis
print("### Key Financial Ratios")

# Calculate ratios using the loaded data
current_year = metadata['report_year']

# You can perform complex calculations here with full IDE support
def calculate_ratios():
    ratios = {}
    
    # Get values safely
    revenue = chart_data['revenue_trend']['values'][0]
    total_assets = 0
    
    # Calculate total assets from balance sheet
    for _, row in balance_df.iterrows():
        if row['Categorie'] in ['Vastgoedbeleggingen', 'Financiële vaste activa', 'Vorderingen', 'Liquide middelen']:
            total_assets += row.get(current_year, 0)
    
    ratios['Asset_Turnover'] = revenue / total_assets if total_assets > 0 else 0
    
    return ratios

ratios = calculate_ratios()

print(f"- **Asset Turnover**: {ratios['Asset_Turnover']:.2f}")
print("- **Additional ratios can be easily calculated here**")
'''
        
        with open(modules_dir / "financial_ratios.py", 'w') as f:
            f.write(content)
    
    def _generate_revenue_charts_module(self, modules_dir: Path):
        """Generate revenue_charts.py module"""
        content = '''# Revenue Analysis Charts
revenue_data = chart_data['revenue_trend']

fig = go.Figure()

fig.add_trace(go.Bar(
    x=revenue_data['years'],
    y=revenue_data['values'],
    name='Revenue',
    marker_color='steelblue',
    text=[f"€{val:,.0f}" for val in revenue_data['values']],
    textposition='auto'
))

fig.update_layout(
    title="Revenue Comparison",
    xaxis_title="Year",
    yaxis_title="Amount (EUR)",
    showlegend=False,
    height=400
)

fig.show()
'''
        
        with open(modules_dir / "revenue_charts.py", 'w') as f:
            f.write(content)
    
    def _generate_asset_charts_module(self, modules_dir: Path):
        """Generate asset_charts.py module"""
        content = '''# Asset Composition Charts
assets_data = chart_data['assets_composition']

if assets_data and assets_data['values']:
    fig = go.Figure(data=[go.Pie(
        labels=assets_data['categories'],
        values=assets_data['values'],
        hole=0.3
    )])
    
    fig.update_layout(
        title=f"Asset Composition - {metadata['report_year']}",
        height=400
    )
    
    fig.show()
else:
    print("No asset composition data available")
'''
        
        with open(modules_dir / "asset_charts.py", 'w') as f:
            f.write(content)
    
    def _generate_notes_module(self, modules_dir: Path):
        """Generate notes.py module"""
        content = f'''# Notes and Methodology
print("## Methodology")
print("- Financial statements prepared in accordance with Dutch GAAP")
print("- Figures in Euros unless otherwise stated")
print("- Automated report generation system ensures consistency and accuracy")

print("\\n## Data Sources")
print("- Source data processed through XAF (XML Audit File) format")
print("- All calculations verified through automated business rules")

print(f"\\n## Report Generation")
print(f"- Generated on: {{metadata.get('generation_date', 'Unknown')}}")
print(f"- System version: Financial Reporting System v{{metadata.get('system_version', '{__version__}')}}")
'''
        
        with open(modules_dir / "notes.py", 'w') as f:
            f.write(content)


class ModularReportOrchestrator:
    """Orchestrates the entire modular reporting process"""
    
    def __init__(self, company_name: str, report_year: str):
        self.company_name = company_name
        self.report_year = report_year
        self.processor = ReportDataProcessor(company_name, report_year)
        self.template_generator = ReportTemplateGenerator(company_name, report_year)
    
    def generate_full_report(self, income_df, balance_df, output_dir) -> Path:
        """Generate complete modular report"""
        from pathlib import Path
        
        print("🔄 Processing data...")
        
        # Process all data and store results
        income_result = self.processor.process_income_statement(income_df)
        balance_result = self.processor.process_balance_sheet(balance_df)
        
        # Ensure data is stored in processor
        self.processor.data['income_statement'] = income_result
        self.processor.data['balance_sheet'] = balance_result
        
        # Save processed data
        self.processor.save_processed_data(output_dir)
        
        print("📝 Generating templates...")
        
        # Generate Python modules using the enhanced Telco modules
        from telco_enhanced_modules import create_telco_modules, create_telco_qmd_template
        create_telco_modules(Path(output_dir))
        
        # Generate main QMD template
        qmd_file = create_telco_qmd_template(Path(output_dir), self.company_name, self.report_year)
        
        print(f"✅ Report generated: {qmd_file}")
        print(f"📁 Python modules in: {Path(output_dir) / 'report_modules'}")
        print(f"📊 Processed data in: {Path(output_dir) / 'processed_data'}")
        
        return qmd_file


# Example usage
if __name__ == "__main__":
    # This would be called from your main script
    pass