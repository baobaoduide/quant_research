import re
import pandas as pd
from old_codes.calc_tools.time_util import cal_date_range, get_change_trade_day
from old_codes.calc_tools.rawdata_util import load_data_from_gogoal, load_data_from_wind_income, load_data_from_wind_notice, load_data_from_wind_express, load_trade_days


def preprocess_ann_data(income_data, express_data, notice_data):
    ann_data = pd.concat([income_data, express_data, notice_data])
    #
    ann_data.sort_values(by=['Code', 'CalcDate', 'ReportPeriod'], inplace=True)
    cummax_report_period = ann_data.groupby(by=['Code'])['ReportPeriod'].transform(lambda x: x.cummax())
    ann_data_use = ann_data[ann_data['ReportPeriod'] == cummax_report_period]
    #
    ann_data_use = ann_data_use.groupby(by=['Code', 'CalcDate'], as_index=False).agg({'ReportPeriod': lambda x: ','.join(x)})
    return ann_data_use


def match_analyst_data_within_date_range(ann_data_use, analyst_forecast_np, trade_days):
    start_date_delta = -1
    end_date_delta = 5
    start_date = pd.to_datetime(
        ann_data_use['CalcDate'].apply(func=lambda x: get_change_trade_day(x, start_date_delta, trade_days)))
    latest_ann_date_after = pd.to_datetime(ann_data_use.groupby(by='Code')['CalcDate'].shift(1)) + pd.Timedelta(
        days=1)
    start_date_adj = pd.concat([start_date, latest_ann_date_after], axis=1).apply(func=lambda x: max(x.dropna()), axis=1)
    ann_data_use['start_date'] = start_date_adj.dt.strftime('%Y-%m-%d')
    end_date = pd.to_datetime(
        ann_data_use['CalcDate'].apply(func=lambda x: get_change_trade_day(x, end_date_delta, trade_days)))
    next_ann_date_before = pd.to_datetime(ann_data_use.groupby(by='Code')['CalcDate'].shift(-1)) - pd.Timedelta(
        days=2)
    end_date_adj = pd.concat([end_date, next_ann_date_before], axis=1).apply(func=lambda x: min(x.dropna()), axis=1)
    ann_data_use['end_date'] = end_date_adj.dt.strftime('%Y-%m-%d')
    #
    ann_data_use['create_date'] = [cal_date_range(s, e).to_list() for s, e in
                                   zip(ann_data_use['start_date'],
                                       ann_data_use['end_date'])]
    ann_date_expand = ann_data_use.explode('create_date').drop(['start_date', 'end_date'], axis=1)
    analyst_forecast_np.sort_values(by=['Code', 'organ_name', 'create_date', 'title', 'report_year'], inplace=True)
    #
    analyst_comment_within_range = pd.merge(ann_date_expand, analyst_forecast_np, on=['Code', 'create_date'])
    analyst_comment_within_range.sort_values(by=['Code', 'CalcDate', 'create_date', 'organ_name'], inplace=True)
    return analyst_comment_within_range


def filter_analyst_data_with_text(analyst_comment_within_range):
    key_words_title = r'(预期|快报|年报|中报|季报|业绩点评|业绩|财报|result|expectations|NPL|Profit|profit|earning|growth|1Q|Q1|3Q|Q3)'
    key_words_content = r'((公布|发布|发表|实现|公告).*(年报|快报|季报|季度报告|利润|收入|业绩)|((收入|利润|业绩).*(增长|下降|预增|预期))|(财测|中报|季报|年报|业绩报告|业绩预告|营业收入占比|季度归母净利润|季度经营现金流|营收)|(report.*(result|profit))|net profit|NPAT|NP)'
    bool_ind_title = analyst_comment_within_range['title'].apply(
        func=lambda x: re.search(key_words_title, x) is not None if isinstance(x, str) else False)
    bool_ind_content = analyst_comment_within_range['content'].apply(
        func=lambda x: re.search(key_words_content, x) is not None if isinstance(x, str) else False)
    analyst_comment_within_range['is_ann_report'] = (bool_ind_title | bool_ind_content).apply(int)
    #
    analyst_match_df = analyst_comment_within_range.drop(columns=['content'])
    analyst_match_df = analyst_match_df.groupby(by=['Code', 'CalcDate', 'ReportPeriod', 'create_date', 'organ_name', 'title'], as_index=False)['is_ann_report'].agg(func=lambda x: int(x.sum() > 0))
    return analyst_match_df


def generate_match(start_cal_date, end_cal_date):
    read_type = 'csv'
    fields_analyst = ['content']
    analyst_forecast_np = load_data_from_gogoal(start_cal_date, end_cal_date, fields_analyst, read_type)
    #
    start_date_ann_all = '2010-01-01'
    end_date_ann_all = '2020-12-31'
    fields_income = []
    income_data = load_data_from_wind_income(start_date_ann_all, end_date_ann_all, fields_income, read_type)
    fields_express = []
    express_data = load_data_from_wind_express(start_date_ann_all, end_date_ann_all, fields_express, read_type)
    fields_notice = []
    notice_data = load_data_from_wind_notice(start_date_ann_all, end_date_ann_all, fields_notice, read_type)
    ann_data_use = preprocess_ann_data(income_data, express_data, notice_data)
    #
    trade_days = load_trade_days(start_date_ann_all, end_date_ann_all, read_type)
    #
    analyst_comment_within_range = match_analyst_data_within_date_range(ann_data_use, analyst_forecast_np, trade_days)
    analyst_match = filter_analyst_data_with_text(analyst_comment_within_range)
    return analyst_match


start_cal_date = '2019-01-01'
end_cal_date = '2020-12-31'
analyst_match = generate_match(start_cal_date, end_cal_date)
path_analyst_match = r'E:\NJU_term4\TF_Intern\EngineerWork\analyst_related_record\analyst_match_2019to2020.csv'
analyst_match.to_csv(path_analyst_match, index=False)
