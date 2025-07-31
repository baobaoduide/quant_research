import os
import pandas as pd
from old_codes.load_data.rawdata_util import load_patent_data_inventory, load_all_stock_basic_info


def test_pt_num_industry(patent_data_inventory, stock_basic_info):
    pt_data_industry = pd.merge(patent_data_inventory, stock_basic_info, on=['Code'])
    pt_data_industry['all_pt_num'] = pt_data_industry['pt_examing_num'] + pt_data_industry['pt_utility_num'] + pt_data_industry['pt_design_num'] + pt_data_industry['pt_authorized_num']
    #
    industry_num_inv = pt_data_industry.groupby(by=['CalcDate', 'industry'])['all_pt_num'].agg(sum='sum', median='median').astype(int)
    industry_num_inv = industry_num_inv.unstack(level=['industry'])
    industry_num_inv.index = pd.DatetimeIndex(industry_num_inv.index)
    industry_num_inv = industry_num_inv.resample('Y').asfreq()
    industry_num_inv.index = industry_num_inv.index.strftime('%Y-%m-%d')
    temp1 = industry_num_inv['sum'].T.sort_values(by=['2020-12-31'], ascending=False)
    temp2 = industry_num_inv['median'].T.sort_values(by=['2020-12-31'], ascending=False)
    #
    pt_data_industry['is_pt'] = (pt_data_industry['all_pt_num'] > 0).astype(int)
    cover_ratio_df = pt_data_industry.groupby(by=['CalcDate', 'industry'])['is_pt'].agg(pt_cover_ratio=lambda x: x.sum() / len(x))
    cover_ratio_df = cover_ratio_df.unstack(level=['industry'])
    cover_ratio_df.index = pd.DatetimeIndex(cover_ratio_df.index)
    cover_ratio_df = cover_ratio_df.resample('Y').asfreq()
    cover_ratio_df.index = cover_ratio_df.index.strftime('%Y-%m-%d')
    temp4 = cover_ratio_df.T.sort_values(by=['2020-12-31'], ascending=False)
    #
    province_num_inv = pt_data_industry.groupby(by=['CalcDate', 'province'])['all_pt_num'].sum()
    province_num_inv = province_num_inv.unstack(level=['province'])
    province_num_inv.index = pd.DatetimeIndex(province_num_inv.index)
    province_num_inv = province_num_inv.resample('Y').asfreq()
    province_num_inv.index = province_num_inv.index.strftime('%Y-%m-%d')
    temp3 = province_num_inv.T.sort_values(by=['2020-12-31'], ascending=False)
    return industry_num_inv, cover_ratio_df


def check_patent_data(start_date, end_date):
    fields_pt_inv = ['pt_examing_num', 'pt_utility_num', 'pt_design_num', 'pt_authorized_num']
    patent_data_inventory = load_patent_data_inventory(start_date, end_date, fields_pt_inv)
    # fields_pt_inc = []
    # patent_data_increase = load_patent_data_increase(start_date, end_date, fields_pt_inc)
    fields_description = ['industry', 'province', 'ipo_date']
    stock_basic_info = load_all_stock_basic_info(fields_description)
    #
    industry_num_inv, cover_ratio_df = test_pt_num_industry(patent_data_inventory, stock_basic_info)
    return industry_num_inv, cover_ratio_df


start_date = '2000-01-01'
end_date = '2020-12-31'
industry_num_inv, cover_ratio_df = check_patent_data(start_date, end_date)
path_industry_num_inv = os.path.join(os.getcwd(), 'industry_num_inv.csv')
industry_num_inv.to_csv(path_industry_num_inv, encoding='utf_8_sig')
path_cover_ratio_df = os.path.join(os.getcwd(), 'ptcover_ratio_df.csv')
cover_ratio_df.to_csv(path_cover_ratio_df, encoding='utf_8_sig')
