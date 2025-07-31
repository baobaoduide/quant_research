import pandas as pd
from old_codes.calc_tools.time_util import get_change_trade_day, cal_trade_date_range


def expand_dates(fee_change_data, trade_days, delta_days_ahead, delta_days_ford):
    fee_change_data['start_date'] = fee_change_data['Date'].apply(func=lambda x: get_change_trade_day(x, -delta_days_ahead, trade_days))
    fee_change_data['end_date'] = fee_change_data['Date'].apply(
        func=lambda x: get_change_trade_day(x, delta_days_ford+1, trade_days))
    fee_change_data.dropna(subset=['start_date', 'end_date'], inplace=True)
    fee_change_data['range_dates'] = [cal_trade_date_range(s, e, trade_days).to_list() for s, e in
                                   zip(fee_change_data['start_date'],
                                       fee_change_data['end_date'])]
    fee_change_expand = fee_change_data.explode('range_dates').drop(['start_date', 'end_date'], axis=1)
    return fee_change_expand


def cal_indicator(close_data):
    backward_df = close_data[close_data['range_dates'] <= close_data['Date']]
    ret_back = backward_df['close'].iloc[-1]/backward_df['close'].iloc[0] - 1
    foreward_df = close_data[close_data['range_dates'] >= close_data['Date']]
    ret_ford = foreward_df['close'].iloc[-1]/foreward_df['close'].iloc[0] - 1
    return_df = pd.DataFrame({'ret_back': ret_back, 'ret_ford': ret_ford}, index=[0])
    return return_df


def prepare_ret_data(fee_change_expand, future_data_trade):
    future_close = future_data_trade[['Date', 'Code', 'close']]
    future_close.rename(columns={'Date': 'range_dates'}, inplace=True)
    all_close_data = pd.merge(fee_change_expand, future_close, on=['Code', 'range_dates'], how='left')
    return_data = all_close_data.groupby(by=['type', 'Code', 'Date']).apply(cal_indicator).reset_index()
    return_data.drop(columns=['level_3'], inplace=True)
    return_data.dropna(subset=['ret_back', 'ret_ford'], inplace=True)
    return_data['year'] = return_data['Date'].apply(func=lambda x: int(x[:4]))
    return return_data


def prepare_data_ret(fee_change_data, future_data_trade, trade_days, delta_days_ahead, delta_days_ford):
    fee_change_expand = expand_dates(fee_change_data, trade_days, delta_days_ahead, delta_days_ford)
    return_data = prepare_ret_data(fee_change_expand, future_data_trade)
    return return_data
