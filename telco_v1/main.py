import sys
import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException
from great_tables import GT
from pathlib import Path
from source import my_excel_lib  

from great_tables.data import sp500


# Define constants
file_path = Path("_data") / "ar23.xlsx"
year = "2023"


def income_statement(file_path: Path, year: str, table_name: str):

    if my_excel_lib.table_exists(file_path, table_name):

        df = my_excel_lib.read_table_from_excel(file_path, table_name)

        df = df.rename(columns={"Note": ""}) # Replace header "Note" with empty string

        gt_table = (
            GT(df)
            .fmt_number(columns=["2022", "2023"], decimals=0, use_seps=True)
            .cols_align(columns=["2022", "2023"], align="right")
            .cols_align(columns=[""], align="center")
        )

        gt_table.show()


if __name__ == "__main__":
    income_statement(file_path, year, "IncomeStatement")
