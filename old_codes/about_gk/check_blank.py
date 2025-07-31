import pandas as pd
from old_codes.about_gk.time_util import adjust_month


def adjust_fore(code, time, profit, quarter, term_id, ana_data_use):
    print(code, time)
    report_year = int(quarter / 10000)
    time_3m = adjust_month(time, -3)
    ana_data = ana_data_use[
        (ana_data_use["Code"] == code)
        & (ana_data_use["create_date"] <= time)
        & (ana_data_use["create_date"] >= time_3m)
    ]
    con_ana = ana_data.groupby(by=["report_year"])["forecast_np"].mean()
    if term_id == 4:
        if report_year in con_ana.index:
            result = con_ana[report_year]
        else:
            result = None
    else:
        if report_year in con_ana.index:
            result = (con_ana[report_year] - profit) / (4 - term_id) * 4
        else:
            result = None
    return result


profit_df = pd.read_csv(r"/Users/xiaotianyu/Desktop/data/profit_month_raw.csv")
ana_data_use = pd.read_csv(r"/Users/xiaotianyu/Desktop/data/ana_forecast_raw2.csv")
ana_data_use.sort_values(
    by=["Code", "create_date", "organ_name", "author_name", "report_year"], inplace=True
)
profit_df = profit_df[profit_df["time"] >= "2009-01-01"]
profit_df["ana_adj"] = profit_df.apply(
    lambda row: adjust_fore(
        row["code"],
        row["time"],
        row["profit_q"],
        row["quarter"],
        row["quarter_ind"],
        ana_data_use,
    ),
    axis=1,
)
