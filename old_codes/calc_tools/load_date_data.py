import os
import pandas as pd


def load_trade_days(start_date, end_date):
    data_dir = r'E:\NJU_term5\project_option\data_raw'
    path_data = os.path.join(data_dir, 'trade_days_china.xlsx')
    rtn = pd.read_excel(path_data, header=None)
    rtn = rtn[0].dt.strftime('%Y-%m-%d')
    rtn = rtn[rtn.between(start_date, end_date)]
    rtn.reset_index(drop=True, inplace=True)
    return rtn
