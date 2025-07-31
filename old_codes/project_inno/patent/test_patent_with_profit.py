import os
import pandas as pd
from old_codes.load_data.load_factor import load_raw_factor_pt
from old_codes.load_data.rawdata_util import load_ashare_financial_indicator, load_all_stock_basic_info


def test_corr(pt_factor, profit_data, all_stock_industry):
    pt_factor['pt_num_all'] = pt_factor['TG3_B001'] + pt_factor['TG3_B002'] + pt_factor['TG3_B003'] + pt_factor['TG3_B004']
    pt_factor['report_year'] = pt_factor['CalcDate'].apply(func=lambda x: int(x[:4]))
    pt_factor['report_month'] = pt_factor['CalcDate'].apply(func=lambda x: int(x[5:7]))
    pt_year_data = pt_factor[pt_factor['report_month'] == 12]
    pt_year_data = pt_year_data[['Code', 'report_year', 'pt_num_all']]
    #
    profit_data['report_year'] = profit_data['ReportPeriod'].apply(func=lambda x: int(x[:4]))
    profit_data['report_quarter'] = profit_data['ReportPeriod'].apply(
        func=lambda x: int(int(x[4:6]) / 3))
    profit_data_use = profit_data[profit_data['report_quarter'] == 4]
    profit_data_use.drop(columns=['report_quarter', 'ReportPeriod', 'CalcDate'], inplace=True)
    profit_data_unstack = profit_data_use.set_index(keys=['report_year', 'Code'])
    profit_data_unstack = profit_data_unstack.unstack(level=['Code']).fillna(0)
    profit_data_unstack['profit'] = profit_data_unstack['profit'].shift(-2) - profit_data_unstack[
        'profit']
    profit_data_year = profit_data_unstack.stack(level='Code').reset_index().dropna(subset=['profit'])
    #
    pt_profit_data = pd.merge(pt_year_data, profit_data_year, on=['Code', 'report_year'])
    pt_profit_industry = pd.merge(pt_profit_data, all_stock_industry[['Code', 'industry']], on=['Code'])
    rd_profit_relation = pt_profit_industry.groupby(by=['report_year', 'industry']).apply(
        func=lambda x: x['pt_num_all'].corr(x['profit'], method='spearman'))
    rd_profit_relation = rd_profit_relation.unstack('industry')
    return rd_profit_relation


def test_pt_profit(start_date, end_date):
    pt_factor = load_raw_factor_pt(start_date, end_date)
    fields_fin = ['profit']
    profit_data = load_ashare_financial_indicator(start_date, end_date, fields_fin)
    fields_description = ['industry']
    all_stock_industry = load_all_stock_basic_info(fields_description)
    #
    corr_pt_profit = test_corr(pt_factor, profit_data, all_stock_industry)
    return corr_pt_profit


start_date = '2010-01-01'
end_date = '2021-05-01'
corr_df = test_pt_profit(start_date, end_date)
path_corr_df = os.path.join(os.getcwd(), 'corr_pt_profit_future.csv')
corr_df.to_csv(path_corr_df, encoding='utf_8_sig')
