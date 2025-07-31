import pandas as pd
from old_codes.calc_tools.time_util import get_change_date_day, cal_date_range
from old_codes.calc_tools.rawdata_util import load_data_from_gogoal


def cal_weight(analyst_forecast_np):
    analyst_forecast_np = analyst_forecast_np.copy()
    analyst_forecast_np['weight'] = 1
    return analyst_forecast_np


def cal_con_forecast_np(analyst_forecast_np_use_sub, start_cal_date, end_cal_date):
    np_use_sub_unstack = analyst_forecast_np_use_sub[['create_date', 'organ_name', 'report_year', 'forecast_np', 'weight']].set_index(keys=['create_date', 'organ_name', 'report_year'])
    np_use_sub_unstack = np_use_sub_unstack.unstack(level=['organ_name', 'report_year'])
    date_range = pd.Index(cal_date_range(start_cal_date, end_cal_date), name='create_date')
    con_look_back_win_size = 90
    np_use_sub_unstack = np_use_sub_unstack.reindex(date_range).fillna(method='pad', axis=0, limit=con_look_back_win_size)
    #
    np_use_sub_unstack = np_use_sub_unstack['forecast_np'] * np_use_sub_unstack['weight']
    np_use_sub_stack_year = np_use_sub_unstack.stack(level='report_year')
    con_forecast_np = np_use_sub_stack_year.mean(axis=1).reset_index()
    con_forecast_np.rename(columns={0: 'con_forecast_np'}, inplace=True)
    return con_forecast_np


def get_con_forecast_np(analyst_forecast_np, start_cal_date, end_cal_date, look_back_win_size):
    analyst_forecast_np.sort_values(by=['Code', 'create_date', 'organ_name', 'author_name'], inplace=True)
    analyst_forecast_np.drop_duplicates(subset=['Code', 'create_date', 'organ_name', 'report_year'], inplace=True)
    analyst_forecast_np = cal_weight(analyst_forecast_np)
    #
    con_forecast_np = analyst_forecast_np.groupby(by=['Code']).apply(
        func=lambda x: cal_con_forecast_np(x, start_cal_date, end_cal_date)).reset_index('Code')
    start_date = get_change_date_day(start_cal_date, look_back_win_size)
    con_forecast_np = con_forecast_np[con_forecast_np['create_date'] >= start_date]
    con_forecast_np.reset_index(drop=True, inplace=True)
    return con_forecast_np


def calc_features(start_cal_date, end_cal_date):
    look_back_win_size = 90
    start_cal_date = get_change_date_day(start_cal_date, -look_back_win_size)
    #
    key_fields_analyst = ['Code', 'organ_name', 'author_name', 'create_date']
    auxiliary_info_fields_analyst = ['report_year', 'report_quarter', 'forecast_np']
    fields_analyst = key_fields_analyst + auxiliary_info_fields_analyst
    analyst_forecast_np = load_data_from_gogoal(start_cal_date, end_cal_date, fields_analyst)
    con_forecast_np = get_con_forecast_np(analyst_forecast_np, start_cal_date, end_cal_date, look_back_win_size)
    return con_forecast_np
