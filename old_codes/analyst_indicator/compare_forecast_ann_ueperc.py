import pandas as pd
from old_codes.calc_tools.time_util import get_change_date_day, cal_date_range
from old_codes.calc_tools.rawdata_util import load_data_from_gogoal, load_data_from_wind_income


def prepare_np_related_values(income_data):
    report_periods = income_data[['Code', 'ReportPeriod']].drop_duplicates()
    report_periods['time_merge'] = pd.to_datetime(report_periods['ReportPeriod'])
    report_periods.sort_values(by='time_merge', inplace=True)
    income_ann_data = income_data.copy()
    income_ann_data['time_merge'] = pd.to_datetime(income_ann_data['CalcDate'])
    income_ann_data.sort_values(by='time_merge', inplace=True)
    report_periods_with_ann_date = pd.merge_asof(report_periods, income_ann_data, on='time_merge',
                                             by=['Code', 'ReportPeriod'], direction='forward',
                                             tolerance=pd.Timedelta('180d'), allow_exact_matches=False)
    #
    report_periods_with_ann_date['report_year'] = report_periods_with_ann_date['ReportPeriod'].apply(func=lambda x: int(x[:4]))
    report_periods_with_ann_date['NP_Quarter'] = report_periods_with_ann_date.groupby(by=['Code', 'report_year'])[
        'NetProfit'].apply(func=lambda x: x - x.shift(1))
    fill_condition = report_periods_with_ann_date['ReportPeriod'].apply(func=lambda x: int(x[4:6]) == 3)
    report_periods_with_ann_date['NP_Quarter'].mask(fill_condition, report_periods_with_ann_date['NetProfit'], inplace=True)
    #
    report_periods_with_ann_date['NP_accum_latest_quarter'] = report_periods_with_ann_date.groupby(by=['Code', 'report_year'])['NetProfit'].shift(1)
    report_periods_with_ann_date['NP_accum_latest_quarter'].mask(fill_condition, 0, inplace=True)
    #
    np_year_condition = report_periods_with_ann_date['ReportPeriod'].apply(func=lambda x: int(x[4:6]) == 12)
    report_periods_with_ann_date['NP_whole_year'] = None
    report_periods_with_ann_date['NP_whole_year'].mask(np_year_condition, report_periods_with_ann_date['NetProfit'], inplace=True)
    report_periods_with_ann_date.sort_values(by=['Code', 'ReportPeriod'], inplace=True)
    report_periods_with_ann_date['NP_whole_year'] = report_periods_with_ann_date.groupby(by=['Code', 'report_year'])['NP_whole_year'].fillna(method='bfill')
    #
    report_periods_with_ann_date.dropna(subset=['CalcDate', 'NP_Quarter'], inplace=True)
    report_periods_with_ann_date.drop(columns='time_merge', inplace=True)
    report_periods_with_ann_date.reset_index(drop=True, inplace=True)
    return report_periods_with_ann_date


def prepare_income_data_recent2year(income_data, start_cal_date, look_back_win_size):
    np_with_related_values = prepare_np_related_values(income_data)
    #
    np_with_related_values_latest_year = np_with_related_values[['Code', 'ReportPeriod', 'NetProfit', 'NP_Quarter', 'NP_whole_year', 'NP_accum_latest_quarter']]
    np_with_related_values_latest_year['ReportPeriod'] = np_with_related_values_latest_year['ReportPeriod'].apply(func=lambda x: str(int(x)+10000))
    np_with_related_values_latest_year.rename(columns={'NP_Quarter': 'NP_Quarter_latest_year', 'NetProfit': 'NetProfit_latest_year', 'NP_whole_year': 'NP_whole_year_latest_year', 'NP_accum_latest_quarter': 'NP_accum_latest_quarter_latest_year'}, inplace=True)
    income_recent2year = pd.merge(np_with_related_values, np_with_related_values_latest_year, on=['Code', 'ReportPeriod'])
    income_recent2year.drop(columns=['NP_whole_year'], inplace=True)
    #
    date_start = get_change_date_day(start_cal_date, look_back_win_size)
    income_recent2year = income_recent2year[income_recent2year['CalcDate'] >= date_start]
    return income_recent2year


def cal_con_forecast_np(analyst_forecast_np_use_sub, start_cal_date, end_cal_date):
    np_use_sub_unstack = analyst_forecast_np_use_sub[['create_date', 'organ_name', 'report_year', 'forecast_np']].set_index(keys=['create_date', 'organ_name', 'report_year'])
    np_use_sub_unstack = np_use_sub_unstack.unstack(level=['organ_name', 'report_year'])
    date_range = pd.Index(cal_date_range(start_cal_date, end_cal_date), name='create_date')
    con_look_back_win_size = 90
    np_use_sub_unstack = np_use_sub_unstack.reindex(date_range).fillna(method='pad', axis=0, limit=con_look_back_win_size)
    #
    np_use_sub_stack_year = np_use_sub_unstack.stack(level='report_year')
    con_forecast_np = np_use_sub_stack_year.mean(axis=1).reset_index()
    con_forecast_np.rename(columns={0: 'con_forecast_np'}, inplace=True)
    start_date = get_change_date_day(start_cal_date, con_look_back_win_size)
    con_forecast_np = con_forecast_np[con_forecast_np['create_date'] >= start_date]
    return con_forecast_np


def get_con_forecast_np(analyst_forecast_np, start_cal_date, end_cal_date):
    analyst_forecast_np_for_con = analyst_forecast_np[analyst_forecast_np['report_quarter'] == 4]
    analyst_forecast_np_for_con = analyst_forecast_np_for_con[analyst_forecast_np_for_con['report_year'] != -1]
    analyst_forecast_np_for_con.sort_values(by=['Code', 'create_date', 'organ_name'], inplace=True)
    analyst_forecast_np_for_con.drop_duplicates(subset=['Code', 'create_date', 'organ_name', 'report_year'], inplace=True)
    #
    con_forecast_np = analyst_forecast_np_for_con.groupby(by=['Code']).apply(
        func=lambda x: cal_con_forecast_np(x, start_cal_date, end_cal_date)).reset_index('Code')
    con_forecast_np.rename(columns={'create_date': 'CalcDate'}, inplace=True)
    con_forecast_np.reset_index(drop=True, inplace=True)
    return con_forecast_np


def cal_ueperc(income_recent2year, con_forecast_np):
    cal_ueperc_df = pd.merge(income_recent2year, con_forecast_np, on=['Code', 'CalcDate', 'report_year'])
    #
    cal_ueperc_df['increase_ratio'] = (cal_ueperc_df['con_forecast_np'] - cal_ueperc_df['NP_accum_latest_quarter']) / (cal_ueperc_df['NP_whole_year_latest_year'] - cal_ueperc_df['NP_accum_latest_quarter_latest_year'])
    cal_ueperc_df['NP_quarter_forecast'] = cal_ueperc_df['increase_ratio'] * cal_ueperc_df['NP_Quarter_latest_year']
    cal_ueperc_df['ueperc'] = (cal_ueperc_df['NP_Quarter'] - cal_ueperc_df['NP_quarter_forecast']) / cal_ueperc_df['NP_quarter_forecast'].abs()
    ueperc_df = cal_ueperc_df[['Code', 'CalcDate', 'ueperc']]
    return ueperc_df


def calc_features(start_cal_date, end_cal_date):
    read_type = 'csv'
    look_back_win_size_income = 640
    start_cal_date_income = get_change_date_day(start_cal_date, -look_back_win_size_income)
    fields_income = ['NetProfit']
    income_data = load_data_from_wind_income(start_cal_date_income, end_cal_date, fields_income, read_type)
    income_recent2year = prepare_income_data_recent2year(income_data, start_cal_date_income, look_back_win_size_income)
    #
    look_back_win_size_analyst = 90
    start_cal_date_analyst = get_change_date_day(start_cal_date, -look_back_win_size_analyst)
    fields_analyst = ['forecast_np']
    analyst_forecast_np = load_data_from_gogoal(start_cal_date_analyst, end_cal_date, fields_analyst, read_type)
    con_forecast_np = get_con_forecast_np(analyst_forecast_np, start_cal_date_analyst, end_cal_date)
    #
    data_value = cal_ueperc(income_recent2year, con_forecast_np)
    return data_value


# start_cal_date = '2020-01-01'
# end_cal_date = '2020-12-31'
# calc_features(start_cal_date, end_cal_date)
