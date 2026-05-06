from great_tables import GT, style, loc

def apply_styles(pl_df_report) -> GT:

    pl_df_report = pl_df_report.rename({
        "Categorie": "_hidden1",
        "Note": "_hidden2"
    })

    gt_tbl = GT(pl_df_report)
    return (
        gt_tbl
        .cols_label(_hidden1="", _hidden2="")
        .cols_align(columns=["_hidden1"], align="left")
        .cols_align(columns=["_hidden2"], align="center")
        .cols_hide(columns=["Sort1", "Sort2","row_type","subtotal_level"])
        .cols_hide(columns=["Level0", "Level1", "Level2"])
        .sub_missing(missing_text="")
        .fmt_number(columns=["2022_detail", "2022_total", "2023_detail", "2023_total"], decimals=0, use_seps=True)
        .tab_style(
            style=style.text(weight="bold"),
            locations=loc.body(rows=[0,1,11]),)
        .tab_style(
            style=style.borders(sides="all", style="hidden"),
            locations=loc.body(rows=[1, 2, 6, 8, 9, 10]),
        )
        .tab_style(
            style=style.borders(sides="bottom", weight=2),
            locations=loc.body(columns=[3, 5], rows=[3]),
        )
        .tab_style(
            style=style.borders(sides="bottom", weight=2),
            locations=loc.body(columns=[4, 6], rows=[4]),
        )
        .tab_style(
            style=style.borders(sides="bottom", weight=2),
            locations=loc.body(columns=[4, 6], rows=[6]),
        )
        .tab_style(
            style=style.borders(sides="bottom", weight=2),
            locations=loc.body(columns=[4, 6], rows=[8]),
        )
        .tab_style(
            style=style.borders(sides="bottom", weight=2),
            locations=loc.body(columns=[4, 6], rows=[10]),
        )
        .tab_style(
            style=style.borders(sides="bottom", style="double", weight=2),
            locations=loc.body(columns=[4, 6], rows=[11]),
        )
    )
