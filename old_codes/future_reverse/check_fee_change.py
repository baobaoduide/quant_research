import os
import pandas as pd
from old_codes.calc_tools.load_future_data import load_future_data_fee


def describe_fee_change(fee_data_sub):
    # fee_data_sub_unique = fee_data_sub
    fee_data_sub_unique = fee_data_sub.drop_duplicates(
        subset=['type', 'Date', 'fee_delta', 'fee_ratio_delta', 'close_discount_ratio'])
    num_fee_change_up = ((fee_data_sub_unique['fee_delta'] > 0) | (fee_data_sub_unique['fee_ratio_delta'] > 0)).sum()
    num_fee_change_down = ((fee_data_sub_unique['fee_delta'] < 0) | (fee_data_sub_unique['fee_ratio_delta'] < 0)).sum()
    num_close_dis_up = (fee_data_sub_unique['closedis_delta'] > 0).sum()
    num_close_dis_down = (fee_data_sub_unique['closedis_delta'] < 0).sum()
    result_df = pd.DataFrame({'num_fee_change_up': num_fee_change_up, 'num_fee_change_down': num_fee_change_down, 'num_close_dis_up': num_close_dis_up, 'num_close_dis_down': num_close_dis_down}, index=[0])
    return result_df


def check_future_fee_data(future_data_fee):
    future_data_fee.sort_values(by=['type', 'Code', 'Date'], inplace=True)
    shift_num = 1
    future_data_fee['fee_last'] = future_data_fee.groupby(by=['type', 'Code'])['fee'].shift(shift_num)
    future_data_fee['fee_delta'] = future_data_fee['fee'] - future_data_fee['fee_last']
    future_data_fee['fee_ratio_last'] = future_data_fee.groupby(by=['type', 'Code'])['fee_ratio'].shift(shift_num)
    future_data_fee['fee_ratio_delta'] = future_data_fee['fee_ratio'] - future_data_fee['fee_ratio_last']
    future_data_fee['fee_close_last'] = future_data_fee.groupby(by=['type', 'Code'])['fee_close'].shift(shift_num)
    future_data_fee['fee_close_delta'] = future_data_fee['fee_close'] - future_data_fee['fee_close_last']
    future_data_fee['closedis_last'] = future_data_fee.groupby(by=['type', 'Code'])['close_discount_ratio'].shift(shift_num)
    future_data_fee['closedis_delta'] = future_data_fee['close_discount_ratio'] - future_data_fee['closedis_last']
    future_data_fee['year'] = future_data_fee['Date'].apply(func=lambda x: int(x[0:4]))
    #
    fee_change_statistic = future_data_fee.groupby(by=['year', 'type']).apply(describe_fee_change)
    fee_change_statistic = fee_change_statistic.droplevel(level=-1)
    four_type_sum = fee_change_statistic.sum(axis=0)
    all_change = fee_change_statistic.sum(axis=1)
    all_change = all_change.unstack(level=['type'])
    all_change['sum_year'] = all_change.sum(axis=1)
    all_change.loc['sum_type'] = all_change.sum(axis=0)
    #
    fee_change_statistic_up = fee_change_statistic['num_fee_change_up'] + fee_change_statistic['num_close_dis_up']
    fee_change_statistic_up_unstack = fee_change_statistic_up.unstack(level=['type'])
    fee_change_statistic_up_unstack['sum_year'] = fee_change_statistic_up_unstack.sum(axis=1).astype(int)
    fee_change_statistic_up_unstack.loc['sum_type'] = fee_change_statistic_up_unstack.sum(axis=0)
    fee_change_statistic_down = fee_change_statistic['num_fee_change_down'] + fee_change_statistic['num_close_dis_down']
    fee_change_statistic_down_unstack = fee_change_statistic_down.unstack(level=['type'])
    fee_change_statistic_down_unstack['sum_year'] = fee_change_statistic_down_unstack.sum(axis=1).astype(int)
    fee_change_statistic_down_unstack.loc['sum_type'] = fee_change_statistic_down_unstack.sum(axis=0)
    return four_type_sum, all_change, fee_change_statistic_up_unstack, fee_change_statistic_down_unstack


def check_fee(start_date, end_date):
    fields_future_fee = []
    future_data_fee = load_future_data_fee(start_date, end_date, fields_future_fee)
    #
    four_type_sum, all_change, fee_change_statistic_up_unstack, fee_change_statistic_down_unstack = check_future_fee_data(future_data_fee)
    return four_type_sum, all_change, fee_change_statistic_up_unstack, fee_change_statistic_down_unstack


four_type_sum, all_change, fee_change_statistic_up_unstack, fee_change_statistic_down_unstack = check_fee('2001-01-01', '2021-10-11')
path_four_type_sum = os.path.join(os.path.dirname(__file__), 'four_type_sum.csv')
four_type_sum.to_csv(path_four_type_sum)
path_all_change = os.path.join(os.path.dirname(__file__), 'all_change.csv')
all_change.to_csv(path_all_change)
path_up_change = os.path.join(os.path.dirname(__file__), 'up_change.csv')
fee_change_statistic_up_unstack.to_csv(path_up_change)
path_down_change = os.path.join(os.path.dirname(__file__), 'down_change.csv')
fee_change_statistic_down_unstack.to_csv(path_down_change)
