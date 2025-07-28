import pandas as pd

from pathlib import Path
from great_tables import GT, md, html
from pathlib import Path

file_path = Path("_data") / "ar23.xlsx"
df = pd.read_excel("_data/ar23.xlsx", sheet_name="2023", usecols="A:E", nrows=13)

# Create a display table showing ten of the largest islands in the world
GT(df).show()