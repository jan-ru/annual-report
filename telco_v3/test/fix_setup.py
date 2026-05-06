#!/usr/bin/env python3
"""
Fix the setup module to ensure variables are properly global
"""
import os
from pathlib import Path

def fix_setup_module():
    """Fix the setup.py module to ensure global variables"""
    print(os.getcwd())
    setup_file = Path("module_tests/report_modules/data_loader.py")
    
    if not setup_file.exists():
        print(f"❌ Setup file not found: {setup_file}")
        return False
    
    # Create the fixed setup content
    fixed_setup = '''# Telco B.V. Report Setup Module - FIXED
import pandas as pd
import json
import numpy as np
from pathlib import Path
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Import Great Tables with error handling
try:
    from great_tables import GT, md, html
    from great_tables import loc, style
except ImportError:
    print("⚠️ Great Tables not available, using fallbacks")
    # Create dummy classes
    class GT:
        def __init__(self, df): self.df = df
        def tab_header(self, *args, **kwargs): return self
        def cols_label(self, *args, **kwargs): return self
        def fmt_currency(self, *args, **kwargs): return self
        def tab_style(self, *args, **kwargs): return self
        def show(self): 
            print("📊 Table Display:")
            print(self.df.head())
    
    class loc:
        @staticmethod
        def body(*args, **kwargs): return {}
    
    class style:
        @staticmethod
        def fill(*args, **kwargs): return {}
        @staticmethod
        def text(*args, **kwargs): return {}
        @staticmethod
        def borders(*args, **kwargs): return {}

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

# CRITICAL: Make variables available in global namespace when using exec()
import builtins
builtins.metadata = metadata
builtins.calculations = calculations
builtins.chart_data = chart_data
builtins.income_df = income_df
builtins.balance_df = balance_df
builtins.current_year = current_year
builtins.previous_year = previous_year
builtins.format_currency_eur = format_currency_eur
builtins.get_safe_value = get_safe_value
builtins.GT = GT
builtins.loc = loc
builtins.style = style
builtins.go = go
builtins.px = px
builtins.pd = pd
builtins.np = np

print(f"✅ Telco B.V. report data loaded for {current_year}")
print(f"📊 Income statement: {income_df.shape[0]} categories")
print(f"📊 Balance sheet: {balance_df.shape[0]} categories")
print("🌐 All variables made globally available")
'''
    
    # Backup and replace
    backup_file = setup_file.with_suffix('.py.backup')
    import shutil
    shutil.copy(setup_file, backup_file)
    
    with open(setup_file, 'w') as f:
        f.write(fixed_setup)
    
    print(f"✅ Fixed setup module (backup saved as {backup_file.name})")
    return True

if __name__ == "__main__":
    fix_setup_module()