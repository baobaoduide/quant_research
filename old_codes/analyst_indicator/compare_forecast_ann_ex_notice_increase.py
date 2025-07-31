import pandas as pd
from old_codes.calc_tools.time_util import cal_date_range, get_change_date_day
from old_codes.calc_tools.rawdata_util import load_data_from_wind_notice, load_data_from_gogoal


def cal_con_np_rolling_change(analyst_forecast_np_use_sub, start_cal_date, end_cal_date):
    np_use_sub_unstack = analyst_forecast_np_use_sub[['create_date', 'organ_name', 'report_year', 'forecast_np']].set_index(keys=['create_date', 'organ_name', 'report_year'])
    np_use_sub_unstack = np_use_sub_unstack.unstack(level=['organ_name', 'report_year'])
    date_range = pd.Index(cal_date_range(start_cal_date, end_cal_date), name='create_date')
    con_look_back_win_size = 90
    np_use_sub_unstack = np_use_sub_unstack.reindex(date_range).fillna(method='pad', axis=0, limit=con_look_back_win_size)
    #
    np_use_sub_stack_year = np_use_sub_unstack.stack(level='report_year')
    con_forecast_np = np_use_sub_stack_year.mean(axis=1)
    con_forecast_np = con_forecast_np.unstack(level='report_year')
    change_win_size = 90
    con_forecast_np_change_ratio = con_forecast_np/con_forecast_np.shift(change_win_size)-1
    con_forecast_np_change_ratio = con_forecast_np_change_ratio.stack(level='report_year').reset_index()
    con_forecast_np_change_ratio.rename(columns={0: 'con_np_change_ratio'}, inplace=True)
    return con_forecast_np_change_ratio


def cal_con_np_up_ratio(notice_data, analyst_forecast_np, start_cal_date, end_cal_date):
    analyst_forecast_np_for_con = analyst_forecast_np[analyst_forecast_np['report_quarter'] == 4]
    analyst_forecast_np_for_con = analyst_forecast_np_for_con[analyst_forecast_np_for_con['report_year'] != -1]
    analyst_forecast_np_for_con.sort_values(by=['Code', 'create_date', 'organ_name'], inplace=True)
    analyst_forecast_np_for_con.drop_duplicates(subset=['Code', 'create_date', 'organ_name', 'report_year'], inplace=True)
    con_np_rolling_change = analyst_forecast_np_for_con.groupby(by=['Code']).apply(func=lambda x: cal_con_np_rolling_change(x, start_cal_date, end_cal_date)).reset_index('Code')
    con_np_rolling_change.rename(columns={'create_date': 'CalcDate'}, inplace=True)
    #
    notice_data['report_year'] = notice_data['ReportPeriod'].apply(func=lambda x: int(x[:4]))
    con_forecast_np_change_ratio = pd.merge(notice_data, con_np_rolling_change, on=['Code', 'CalcDate', 'report_year'])
    con_forecast_np_change_ratio['is_notice_ex'] = (con_forecast_np_change_ratio['ProfitChange_Min'] > con_forecast_np_change_ratio['con_np_change_ratio']).apply(int)
    is_notice_ex_ana = con_forecast_np_change_ratio[['Code', 'CalcDate', 'is_notice_ex']]
    return is_notice_ex_ana


def calc_features(start_cal_date, end_cal_date):
    read_type = 'csv'
    fields_notice = ['ProfitChange_Min']
    notice_data = load_data_from_wind_notice(start_cal_date, end_cal_date, fields_notice, read_type)
    #
    look_back_win_size = 180
    start_cal_date = get_change_date_day(start_cal_date, -look_back_win_size)
    fields_analyst = ['forecast_np']
    analyst_forecast_np = load_data_from_gogoal(start_cal_date, end_cal_date, fields_analyst, read_type)
    #
    is_notice_ex_ana = cal_con_np_up_ratio(notice_data, analyst_forecast_np, start_cal_date, end_cal_date)
    return is_notice_ex_ana


# start_cal_date = '2020-01-01'
# end_cal_date = '2020-12-31'
# calc_features(start_cal_date, end_cal_date)
