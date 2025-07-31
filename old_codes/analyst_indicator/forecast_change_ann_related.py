import pandas as pd
from old_codes.calc_tools.time_util import get_change_trade_day
from old_codes.calc_tools.rawdata_util import load_data_from_gogoal, load_analyst_match, load_trade_days


def match_analyst_forecast_np(analyst_match, analyst_forecast_np):
    analyst_match = analyst_match[analyst_match['is_ann_report'] == 1]
    analyst_match_forecast_np = pd.merge(analyst_match, analyst_forecast_np, on=['Code', 'organ_name', 'title', 'create_date'])
    #
    analyst_match_forecast_np = analyst_match_forecast_np[(analyst_match_forecast_np['report_year'] != -1) | (analyst_match_forecast_np['report_quarter'] == 4)]
    analyst_match_forecast_np.drop(columns=['is_ann_report', 'report_quarter'], inplace=True)
    analyst_match_forecast_np.dropna(subset=['forecast_np'], inplace=True)
    analyst_match_forecast_np.reset_index(drop=True, inplace=True)
    return analyst_match_forecast_np


def cal_analyst_adjust_ud(analyst_match_forecast_np, trade_days):
    analyst_match_forecast_np['forecast_np_adjust'] = analyst_match_forecast_np.groupby(by=['Code', 'organ_name', 'report_year'])['forecast_np'].apply(func=lambda x: x - x.shift(1))
    analyst_adjust = analyst_match_forecast_np.dropna(subset=['forecast_np_adjust'])
    analyst_adjust['aim_year'] = analyst_adjust['ReportPeriod'].apply(
        func=lambda x: int(x[:4]) + 1 if x[4:6] == '12' else int(x[:4]))
    analyst_adjust = analyst_adjust[analyst_adjust['report_year'] == analyst_adjust['aim_year']]
    #
    calcdate_delta = 5
    analyst_adjust['CalcDate'] = analyst_adjust['CalcDate'].apply(func=lambda x: get_change_trade_day(x, calcdate_delta, trade_days))
    func_np_adjust_is_any = lambda x: int(sum((x > 0).apply(int)) > 0)
    number_limit = 3
    func_np_adjust_is_all = lambda x: int((sum((x > 0).apply(int)) == len(x))) & (len(x) > number_limit)
    func_udpct = lambda x: (sum((x > 0).apply(int)) - sum((x < 0).apply(int))) / len(x)
    analyst_adjust_ud = analyst_adjust.groupby(by=['Code', 'CalcDate'], as_index=False)['forecast_np_adjust'].agg({'forecast_np_adjust_is_any': func_np_adjust_is_any, 'forecast_np_adjust_is_all': func_np_adjust_is_all, 'udpct': func_udpct})
    return analyst_adjust_ud


def calc_features(start_cal_date, end_cal_date):
    analyst_match = load_analyst_match(start_cal_date, end_cal_date)
    #
    read_type = 'csv'
    fields_analyst = ['forecast_np']
    analyst_forecast_np = load_data_from_gogoal(start_cal_date, end_cal_date, fields_analyst, read_type)
    #
    analyst_match_forecast_np = match_analyst_forecast_np(analyst_match, analyst_forecast_np)
    #
    trade_days = load_trade_days(start_cal_date, end_cal_date, read_type)
    analyst_adjust_ud = cal_analyst_adjust_ud(analyst_match_forecast_np, trade_days)
    return analyst_adjust_ud


start_cal_date = '2020-01-01'
end_cal_date = '2020-12-31'
calc_features(start_cal_date, end_cal_date)
