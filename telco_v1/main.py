import pandas as pd
import polars as pl
from great_tables import GT
from pathlib import Path
from source import my_excel_lib
from my_excel_lib.mappings import balance_sheet_mapping
from my_excel_lib.mappings import profit_loss_mapping
from my_excel_lib.styles import profit_loss_style
from my_excel_lib.styles import balance_sheet_style


# Init constants
file_path = Path("_data") / "ar23.xlsx"

def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitizes a DataFrame for Polars compatibility:
    - Assumes columns 3 to 6 (inclusive) must be converted to NumPy-backed int64.
    - Leaves all other columns unchanged.
    - Raises an error if any column retains a non-standard dtype.

    Args:
        df (pd.DataFrame): The input DataFrame to sanitize.

    Returns:
        pd.DataFrame: A sanitized DataFrame with consistent dtypes.

    Raises:
        TypeError: If any column retains a non-NumPy dtype after sanitization.
    """
    df = df.copy()
    target_cols = df.columns[2:]  # columns 3 to 6 (inclusive, 0-based)

    for col in target_cols:
        df.loc[:, col] = (
            pd.to_numeric(df[col], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

    # Check for non-NumPy dtypes
    allowed = {"int64", "float64", "object", "bool"}
    non_numpy_dtypes = df.dtypes[~df.dtypes.apply(lambda dt: dt.name in allowed)]

    if not non_numpy_dtypes.empty:
        raise TypeError(
            f"Non-NumPy dtypes found:\n{non_numpy_dtypes.to_string()}\n"
            "Ensure all columns use NumPy-backed dtypes."
        )

    return df


def read(filepath: Path, table_name: str):
    if my_excel_lib.table_exists(file_path, table_name):
        df_data = my_excel_lib.read_table_from_excel(file_path, table_name)
    return df_data


def mapping(df_data: pd.DataFrame, df_map: pd.DataFrame) -> pd.DataFrame:

    df_data = sanitize_df(df_data)

    pl_df_data = pl.from_pandas(df_data)
    pl_df_map = pl.from_pandas(df_map)

    pl_df_report = balance_sheet_mapping.calculate_subtotals_with_insertion_control(pl_df_data, pl_df_map)
    df_report = pl_df_report.to_pandas()

    return df_report


def render(df_report: pd.DataFrame, style_name: str) -> GT:
        
    # Create a mapping dictionary
    style_modules = {
        'profit_loss': profit_loss_style,
        'assets': assets_style,
        'liabilities': liabilities_style
    }

    df_display = df_report.rename(columns={
        "Categorie": "_hidden1",
        "Note": "_hidden2"}
    )

    # Get the appropriate style module
    style_module = style_modules.get(style_name)
    if style_module is None:
        raise ValueError(f"Unknown style_name: {style_name}. Available: {list(style_modules.keys())}")

    styled_table = style_module.apply_styles(GT(df_display))
    return styled_table


if __name__ == "__main__":
    """
    Generate a styled financial report based on a selected financial object.

    This script allows selecting one of several financial objects (e.g., 'assets',
    'liabilities', or 'profit_loss') and dynamically loads the corresponding Excel data, 
    mapping definitions, and styling instructions to produce a formatted report.

    Each financial object is defined in the `financial_objects` dictionary with:
    - `table_name`: name of the Excel worksheet containing the raw data.
    - `mapping_name`: name of the worksheet containing the mapping logic.
    - `style_name`: a label passed to the style renderer.
    - `mapping_module`: the Python module that provides the mapping function.
    - `style_module`: the Python module that provides the styling function.

    The script performs the following steps:
    1. Loads configuration for the selected financial object.
    2. Dynamically imports the mapping and styling modules.
    3. Loads the relevant Excel data and mapping.
    4. Applies the mapping transformation.
    5. Applies the styling using a custom render function.
    6. Displays the final report.

    Notes
    -----
    - Assumes all mapping modules implement a `mapping(df_data, df_map)` function.
    - Assumes all style modules implement a `render(df_report, style_name)` function.
    - Column name fix is applied to correct possible renaming of 'Categorie' by pandas.

    Examples
    --------
    To generate a profit & loss report:

    >>> selected_object = "profit_loss"
    >>> # run the script
    """


# Define all configurations for financial objects
financial_objects = {
    "assets": {
        "table_name": "tbl_assets_data",
        "mapping_name": "tbl_assets_mapping",
        #"style_name": "assets",
        "mapping_module": balance_sheet_mapping,
        "style_module": balance_sheet_style
    },
    "liabilities": {
        "table_name": "tbl_liabilities_data",
        "mapping_name": "tbl_liabilities_mapping",
        #"style_name": "liabilities",
        "mapping_module": balance_sheet_mapping,
        "style_module": balance_sheet_style
    },
    "profit_loss": {
        "table_name": "tbl_profit_loss_data",
        "mapping_name": "tbl_profit_loss_mapping",
        #"style_name": "profit_loss",
        "mapping_module": profit_loss_mapping,
        "style_module": profit_loss_style
    }
}

# --- Select financial object here ---
selected_object = "liabilities"  # Change to 'assets' or 'liabilities' as needed

if selected_object not in financial_objects:
    raise ValueError(f"Unknown financial object: {selected_object}")

config = financial_objects[selected_object]

# --- Import modules ---
# mapping_module = importlib.import_module(mapping_module_path)
# style_module = importlib.import_module(style_module_path)

# --- Read data ---
df_data = read(file_path, config["table_name"])
df_map = read(file_path, config["mapping_name"])
df_map = df_map.rename(columns={"Categorie.1": "Categorie"})  # Fix for duplicate column name

# --- Cnvert to polars ---
pl_df_data = pl.from_pandas(df_data)
pl_df_map = pl.from_pandas(df_map)

# --- Apply mapping and styling ---
print(pl_df_data.head())  # Debug: Show first few rows of data
print(pl_df_map.head())  # Debug: Show first few rows of mapping
pl_df_report = config["mapping_module"].calculate_subtotals(pl_df_data, pl_df_map)
print(pl_df_report)
styled_table = config["style_module"].apply_styles(pl_df_report)
styled_table.show()
