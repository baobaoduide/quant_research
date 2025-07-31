import os
import pandas as pd
from old_codes.calc_tools.time_util import get_change_trade_day
from old_codes.calc_tools.load_future_data import load_future_data_fee, load_future_data_trade
from old_codes.calc_tools.load_date_data import load_trade_days
from prepare_fee_data_use import prepare_fee_data


def prepare_rdp_data(fee_change_data, future_data_trade, trade_days, delta_days_ahead):
    fee_change_data['start_date'] = fee_change_data['Date'].apply(
        func=lambda x: get_change_trade_day(x, -delta_days_ahead, trade_days))
    future_close = future_data_trade[['Date', 'Code', 'close']]
    future_close.rename(columns={'Date': 'start_date', 'close': 'close_start'}, inplace=True)
    all_close_data = pd.merge(fee_change_data, future_close, on=['Code', 'start_date'], how='left')
    all_close_data = pd.merge(all_close_data, future_data_trade[['Date', 'Code', 'close']], on=['Code', 'Date'], how='left')
    all_close_data['ret'] = all_close_data['close'] / all_close_data['close_start'] - 1
    rdp_data = all_close_data[['type', 'Code', 'Date', 'ret']]
    return rdp_data


def check_rdp_data(rdp_data):
    check_data = rdp_data.groupby(by=['type', 'Date'])['ret'].mean()
    check_data_unstack = check_data.unstack(level=['type'])
    return check_data_unstack


def check_back_udr(start_date, end_date, delta_days_ahead):
    fields_future_fee = []
    future_data_fee = load_future_data_fee(start_date, end_date, fields_future_fee)
    fields_future_trade = []
    future_data_trade = load_future_data_trade(start_date, end_date, fields_future_trade)
    trade_day_start = '1990-01-01'
    trade_day_end = '2021-09-29'
    trade_days = load_trade_days(trade_day_start, trade_day_end)
    #
    adjust_type = 'up'
    fee_change_data = prepare_fee_data(future_data_fee, adjust_type)
    # delta_days_ahead = 10
    rdp_data = prepare_rdp_data(fee_change_data, future_data_trade, trade_days, delta_days_ahead)
    result = check_rdp_data(rdp_data)
    return result


num_back_days = [5, 10, 20]
for i in range(len(num_back_days)):
    delta_days_ahead = num_back_days[i]
    result = check_back_udr('2001-01-01', '2021-10-11', delta_days_ahead)
    path_result = os.path.join(os.path.dirname(__file__), 'rdr_'+str(delta_days_ahead)+'back.csv')
    result.to_csv(path_result)
