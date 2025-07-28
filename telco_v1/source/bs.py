#| echo: false

import polars as pl
from great_tables import GT, md, html
from great_tables.data import islands

islands_mini = (
    pl.from_pandas(islands).sort("size", descending=True)
    .head(10)
)

gt_ex = (
    GT(islands_mini)
    .tab_header(
        title="Large Landmasses of the World",
    )
    .tab_stub(rowname_col="name")
    .tab_source_note(source_note="Source: The World Almanac and Book of Facts, 1975, page 406.")
    
    .tab_stubhead(label="landmass")
    .fmt_integer(columns="size")
)

gt_ex
