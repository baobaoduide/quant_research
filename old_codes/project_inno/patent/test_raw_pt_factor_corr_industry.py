import os
import pandas as pd
from old_codes.load_data.rawdata_util import load_all_stock_basic_info


def cal_corr(factor_df_date):
    corr_df_date = factor_df_date.iloc[:, 2:]
    corr_df_date = corr_df_date.corr()
    return corr_df_date


def cal_factor_corr(factor_df, asharedescription):
    factor_df = pd.merge(factor_df, asharedescription, on=['Code'])
    factor_corr = factor_df.groupby(by=['CalcDate', 'industry']).apply(cal_corr)
    end_factor_corr = factor_corr.loc['2020-12-31']
    factor_corr_mean = factor_corr['2011-01-01':]
    # factor_corr_mean.reset_index(level=-1, inplace=True)
    factor_corr_mean = factor_corr_mean.groupby(level=[1, 2]).mean()
    return factor_corr, end_factor_corr, factor_corr_mean


def test_pt_factor_cover(start_date, end_date):
    path_pt_factor = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\factor_pt_num_inc.csv'
    factor_df = pd.read_csv(path_pt_factor)
    factor_df = factor_df[(factor_df['CalcDate'] >= start_date) & (factor_df['CalcDate'] <= end_date)]
    fields_description = ['industry']
    asharedescription = load_all_stock_basic_info(fields_description)
    #
    factor_corr, end_factor_corr, factor_corr_mean = cal_factor_corr(factor_df, asharedescription)
    return factor_corr, end_factor_corr, factor_corr_mean


start_date = '2004-01-01'
end_date = '2020-12-31'
factor_corr, end_factor_corr, factor_corr_mean = test_pt_factor_cover(start_date, end_date)
path_factor_corr = os.path.join(os.getcwd(), 'pt_factor_corr_industry.csv')
factor_corr.to_csv(path_factor_corr, encoding='utf_8_sig')
path_end_factor_corr = os.path.join(os.getcwd(), 'pt_end_factor_corr_industry.csv')
end_factor_corr.to_csv(path_end_factor_corr, encoding='utf_8_sig')
path_factor_corr_mean = os.path.join(os.getcwd(), 'pt_factor_corr_mean_industry.csv')
factor_corr_mean.to_csv(path_factor_corr_mean, encoding='utf_8_sig')


industrys = ['医药', '机械', '电子', '汽车', '通信', '计算机', '基础化工', '电力设备及新能源', '国防军工', '家电', '轻工制造']
for i in range(len(industrys)):
    industry = industrys[i]
    check_df = factor_corr_mean.loc[industry]
