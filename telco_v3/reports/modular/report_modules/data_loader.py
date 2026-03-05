# Telco B.V. Report Setup Module
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
