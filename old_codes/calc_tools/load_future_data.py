import os
import pandas as pd
folder = r'E:\NJU_term5\TF_Intern\project_future\rawdata'


def load_future_data_fee(start_date, end_date, flds):
    path_data = os.path.join(folder, 'future_data_fee_20211011.xlsx')
    rtn = pd.read_excel(path_data, dtype={'日期': str})
    rtn.rename(columns={'日期': 'Date', '合约代码': 'Code', '交易手续费额（元/手）': 'fee', '交易手续费率（%）': 'fee_ratio', '平今仓手续费（元/手）': 'fee_close', '平今折扣率（%）': 'close_discount_ratio', '所属品种': 'type'}, inplace=True)
    rtn['Date'] = rtn['Date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn['close_discount_ratio'] = rtn['close_discount_ratio'] / 100
    rtn['fee_ratio'] = rtn['fee_ratio'] / 100
    rtn = rtn[rtn['Date'].between(start_date, end_date)]
    key_cols = ['Code', 'Date']
    # rtn = rtn[key_cols + flds]
    rtn.sort_values(by=key_cols, inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_future_data_trade(start_date, end_date, flds):
    path_data = os.path.join(folder, 'future_data_trade.csv')
    rtn = pd.read_csv(path_data, dtype={'trade_dt': str})
    rtn.rename(columns={'trade_dt': 'Date', 'windcode': 'Code', 'shortcode': 'type'}, inplace=True)
    rtn['Date'] = rtn['Date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn = rtn[rtn['Date'].between(start_date, end_date)]
    key_cols = ['Code', 'Date']
    # rtn = rtn[key_cols + flds]
    rtn.sort_values(by=key_cols, inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


# load_future_data_fee('2000-01-01', '2021-01-01', [])
load_future_data_trade('2000-01-01', '2021-01-01', [])
