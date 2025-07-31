import os
import pandas as pd
from old_codes.calc_tools.load_future_data import load_future_data_fee, load_future_data_trade
from old_codes.calc_tools.load_date_data import load_trade_days
from prepare_fee_data_use import prepare_fee_data
from prepare_ret_data import prepare_data_ret


def count_times(return_data_sub):
    return_data_sub = return_data_sub.drop_duplicates(subset=['type', 'Date'])
    times = len(return_data_sub)
    return times


def check_relation_overall(return_data, delta_days):
    corr_data = return_data['ret_back'].corr(return_data['ret_ford'], method='pearson')
    corr_data_rank = return_data['ret_back'].corr(return_data['ret_ford'], method='spearman')
    #
    type_count = return_data.groupby(by=['type']).apply(count_times)
    type_corr = return_data.groupby(by=['type']).apply(func=lambda x: x['ret_back'].corr(x['ret_ford']))
    type_result_all = pd.concat([type_count, type_corr], axis=1)
    type_result_all.rename(columns={0: 'num%d' % delta_days, 1: 'corr%d' % delta_days}, inplace=True)
    return corr_data, corr_data_rank


def find_best_para(fee_change_data, future_data_trade, trade_days):
    delta_days_list = list(range(5, 65, 5))
    # delta_days_list = [15]
    corr_data_list = []
    corr_rank_data_list = []
    for i in range(len(delta_days_list)):
        delta_days_a = delta_days_list[i]
        delta_days_f = delta_days_a
        return_data = prepare_data_ret(fee_change_data, future_data_trade, trade_days, delta_days_a, delta_days_f)
        corr_data_i, corr_data_rank_i = check_relation_overall(return_data, delta_days_a)
        corr_data_list.append(corr_data_i)
        corr_rank_data_list.append(corr_data_rank_i)
    corr_data_all = pd.DataFrame(corr_data_list, index=delta_days_list)
    corr_rank_data_all = pd.DataFrame(corr_rank_data_list, index=delta_days_list)
    return corr_data_all, corr_rank_data_all


def check_reverse_relation(start_date, end_date):
    fields_future_fee = []
    future_data_fee = load_future_data_fee(start_date, end_date, fields_future_fee)
    fields_future_trade = []
    future_data_trade = load_future_data_trade(start_date, end_date, fields_future_trade)
    trade_day_start = '1990-01-01'
    trade_day_end = '2021-09-29'
    trade_days = load_trade_days(trade_day_start, trade_day_end)
    #
    adjust_types = ['up', 'down']
    for i in range(len(adjust_types)):
        adjust_type = adjust_types[i]
        fee_change_data = prepare_fee_data(future_data_fee, adjust_type)
        corr_data_all, corr_rank_data_all = find_best_para(fee_change_data, future_data_trade, trade_days)
        path_corr_data = os.path.join(os.path.dirname(__file__), 'corr_overall_'+adjust_type+'.csv')
        corr_data_all.to_csv(path_corr_data)
        path_corr_rank_data_all = os.path.join(os.path.dirname(__file__), 'corr_rank_data_all_'+adjust_type+'.csv')
        corr_rank_data_all.to_csv(path_corr_rank_data_all)
    return corr_data_all, corr_rank_data_all


check_reverse_relation('2001-01-01', '2021-10-11')
