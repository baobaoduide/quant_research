import os.path
import pandas as pd
from old_codes.calc_tools.load_future_data import load_future_data_fee, load_future_data_trade
from old_codes.calc_tools.load_date_data import load_trade_days
from old_codes.calc_tools.time_util import get_change_trade_day, cal_trade_date_range


def prepare_future_fee_data(future_data_fee):
    future_data_fee.sort_values(by=['type', 'Code', 'Date'], inplace=True)
    shift_num = 1
    future_data_fee['fee_last'] = future_data_fee.groupby(by=['type', 'Code'])['fee'].shift(shift_num)
    future_data_fee['fee_delta'] = future_data_fee['fee'] - future_data_fee['fee_last']
    future_data_fee['fee_ratio_last'] = future_data_fee.groupby(by=['type', 'Code'])['fee_ratio'].shift(shift_num)
    future_data_fee['fee_ratio_delta'] = future_data_fee['fee_ratio'] - future_data_fee['fee_ratio_last']
    future_data_fee['fee_close_last'] = future_data_fee.groupby(by=['type', 'Code'])['fee_close'].shift(shift_num)
    future_data_fee['fee_close_delta'] = future_data_fee['fee_close'] - future_data_fee['fee_close_last']
    #
    fee_change_data = future_data_fee[(future_data_fee['fee_delta'] > 0) | (future_data_fee['fee_ratio_delta'] > 0)]
    fee_change_data = fee_change_data[['type', 'Code', 'Date']]
    fee_change_data.reset_index(drop=True, inplace=True)
    return fee_change_data


def expand_dates(fee_change_data, trade_days, delta_days):
    fee_change_data['start_date'] = fee_change_data['Date'].apply(func=lambda x: get_change_trade_day(x, -delta_days, trade_days))
    fee_change_data['end_date'] = fee_change_data['Date'].apply(
        func=lambda x: get_change_trade_day(x, delta_days+1, trade_days))
    fee_change_data.dropna(subset=['start_date', 'end_date'], inplace=True)
    fee_change_data['range_dates'] = [cal_trade_date_range(s, e, trade_days).to_list() for s, e in
                                   zip(fee_change_data['start_date'],
                                       fee_change_data['end_date'])]
    fee_change_expand = fee_change_data.explode('range_dates').drop(['start_date', 'end_date'], axis=1)
    return fee_change_expand


def cal_indicator(close_data, shift_num):
    close_data['ret'] = close_data['close'] / close_data['close'].shift(shift_num) - 1
    backward_df = close_data[close_data['range_dates'] <= close_data['Date']]
    foreward_df = close_data[close_data['range_dates'] > close_data['Date']]
    return_df = pd.DataFrame({'ret_back': backward_df['ret'].iloc[-1], 'ret_ford': foreward_df['ret'].iloc[-1]}, index=[0])
    return return_df


def check_relation(fee_change_expand, future_data_trade, delta_days):
    future_close = future_data_trade[['Date', 'Code', 'close']]
    future_close.rename(columns={'Date': 'range_dates'}, inplace=True)
    all_close_data = pd.merge(fee_change_expand, future_close, on=['Code', 'range_dates'], how='left')
    return_data = all_close_data.groupby(by=['type', 'Code', 'Date']).apply(func= lambda x: cal_indicator(x, delta_days)).reset_index()
    return_data.drop(columns=['level_3'], inplace=True)
    return_data.dropna(subset=['ret_back', 'ret_ford'], inplace=True)
    #
    corr_data = return_data['ret_back'].corr(return_data['ret_ford'], method='pearson')
    corr_data_rank = return_data['ret_back'].corr(return_data['ret_ford'], method='spearman')
    #
    type_count = return_data.groupby(by='type')['ret_back'].count()
    type_corr = return_data.groupby(by='type').apply(func=lambda x: x['ret_back'].corr(x['ret_ford']))
    type_result_all = pd.concat([type_count, type_corr], axis=1).rename(columns={'ret_back': 'num', 0: 'corr'})
    type_result = type_result_all[type_result_all['num'] >= 10]
    type_result_all.rename(columns={'num': 'num%d' % delta_days, 'corr': 'corr%d' % delta_days}, inplace=True)
    #
    return_data_time = return_data.sort_values(by=['Date', 'type', 'Code'])
    time_count = return_data_time.groupby(by=['Date'])['ret_back'].count()
    time_corr = return_data_time.groupby(by=['Date']).apply(func=lambda x: x['ret_back'].corr(x['ret_ford']))
    time_corr_result_all = pd.concat([time_count, time_corr], axis=1).rename(columns={'ret_back': 'num', 0: 'corr'})
    time_corr_result = time_corr_result_all[time_corr_result_all['num'] >= 5]
    time_corr_result_all.rename(columns={'num': 'num%d' % delta_days, 'corr': 'corr%d' % delta_days}, inplace=True)
    return corr_data, type_result_all, time_corr_result_all


def find_best_para(fee_change_data, future_data_trade, trade_days):
    delta_days_list = list(range(5, 65, 5))
    # delta_days_list = [5, 10, 15]
    corr_data_list = []
    type_result_list = []
    time_corr_result_list = []
    for i in range(len(delta_days_list)):
        delta_days = delta_days_list[i]
        fee_change_expand = expand_dates(fee_change_data, trade_days, delta_days)
        corr_data_i, type_result_i, time_corr_result_i = check_relation(fee_change_expand, future_data_trade, delta_days)
        corr_data_list.append(corr_data_i)
        type_result_list.append(type_result_i)
        time_corr_result_list.append(time_corr_result_i)
    corr_data_all = pd.DataFrame(corr_data_list, index=delta_days_list)
    type_result_all = pd.concat(type_result_list, axis=1)
    time_corr_result_all = pd.concat(time_corr_result_list, axis=1)
    return corr_data_all, type_result_all, time_corr_result_all


def check_reverse_relation(start_date, end_date):
    fields_future_fee = []
    future_data_fee = load_future_data_fee(start_date, end_date, fields_future_fee)
    fields_future_trade = []
    future_data_trade = load_future_data_trade(start_date, end_date, fields_future_trade)
    trade_day_start = '1990-01-01'
    trade_day_end = '2021-09-29'
    trade_days = load_trade_days(trade_day_start, trade_day_end)
    #
    fee_change_data = prepare_future_fee_data(future_data_fee)
    corr_data, type_result_all, time_corr_result_all = find_best_para(fee_change_data, future_data_trade, trade_days)
    return corr_data, type_result_all, time_corr_result_all


corr_data, type_result_all, time_corr_result_all = check_reverse_relation('2001-01-01', '2021-10-11')
path_corr_data = os.path.join(os.path.dirname(__file__), 'corr_overall.csv')
corr_data.to_csv(path_corr_data)
path_type_result = os.path.join(os.path.dirname(__file__), 'corr_type.csv')
type_result_all.to_csv(path_type_result)
path_time_corr_result = os.path.join(os.path.dirname(__file__), 'corr_time.csv')
time_corr_result_all.to_csv(path_time_corr_result)
