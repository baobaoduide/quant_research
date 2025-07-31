import os
import numpy as np
import pandas as pd
import statsmodels.api as sm
from old_codes.load_data.rawdata_util import load_mv_data, load_all_stock_basic_info
from old_codes.calc_tools.time_util import get_change_date_day
from old_codes.load_data.load_factor import load_raw_factor_pt


def prepare_factor_df(factor_df, ashare_mv, asharedescription):
    delta_date_cut = 180
    asharedescription['date_cut'] = asharedescription['ipo_date'].apply(func=lambda x: get_change_date_day(x, delta_date_cut))
    mv_industry_df = pd.merge(ashare_mv, asharedescription[['Code', 'industry', 'date_cut']], on='Code')
    mv_industry_df = mv_industry_df[mv_industry_df['CalcDate'] > mv_industry_df['date_cut']]
    mv_industry_df.drop(columns=['date_cut'], inplace=True)
    #
    factor_df_adjust_freq = pd.merge(mv_industry_df, factor_df, on=['Code', 'CalcDate'], how='left')
    factor_df_adjust_freq = factor_df_adjust_freq.groupby(by=['Code'], as_index=False).apply(lambda x: x.ffill())
    factor_df_adjust_freq.dropna(subset=['market_value'], inplace=True)
    # factor_df_adjust_freq = factor_df_adjust_freq[factor_df_adjust_freq['industry'].isin(industrys)]
    return factor_df_adjust_freq


def removeoutlier_and_normalize(factor):
    mean = factor.mean()
    std = factor.std()
    upper_bound = mean + 3*std
    lower_bound = mean - 3*std
    factor = factor.mask(factor > upper_bound, upper_bound)
    factor = factor.mask(factor < lower_bound, lower_bound)
    #
    mean = factor.mean()
    std = factor.std()
    normalized_factor = (factor - mean) / std
    return normalized_factor


def std_process_industry(factor_df_industry_sub):
    normalized_mv = removeoutlier_and_normalize(np.log(factor_df_industry_sub['market_value']))
    x = sm.add_constant(normalized_mv)
    fill_value = factor_df_industry_sub['factor'].mean()
    factor = factor_df_industry_sub['factor'].fillna(fill_value)
    factor = removeoutlier_and_normalize(factor)
    results = sm.OLS(factor, x).fit()
    factor_df_industry_sub['factor'] = removeoutlier_and_normalize(results.resid)
    return factor_df_industry_sub


def cal_std_factor(factor_df_origin):
    columns = factor_df_origin.columns.to_list()
    factor_df_std_all = factor_df_origin[['industry', 'CalcDate', 'Code']]
    for i in range(4, len(columns)):
        column = columns[i]
        com_col_list = columns[:4]
        com_col_list.append(column)
        std_df = factor_df_origin[com_col_list]
        std_df.rename(columns={column: 'factor'}, inplace=True)
        factor_df_std = std_df.groupby(by=['industry', 'CalcDate']).apply(std_process_industry).reset_index()
        factor_df_std.drop(columns=['index'], inplace=True)
        factor_df_std.rename(columns={'factor': column}, inplace=True)
        factor_df_std_all = pd.merge(factor_df_std_all, factor_df_std[['industry', 'CalcDate', 'Code', column]], on=['industry', 'CalcDate', 'Code'])
    return factor_df_std_all


def std_factor(start_date, end_date):
    factor_df = load_raw_factor_pt(start_date, end_date)
    #
    fileds_mv = ['market_value']
    freq = 'M'
    ashare_mv = load_mv_data(start_date, end_date, fileds_mv, freq)
    fields_description = ['ipo_date', 'industry']
    asharedescription = load_all_stock_basic_info(fields_description)
    #
    factor_df_stack = prepare_factor_df(factor_df, ashare_mv, asharedescription)
    factor_df_std = cal_std_factor(factor_df_stack)
    return factor_df_std


start_date = '2010-01-01'
end_date = '2020-12-31'
industrys = ['医药', '机械', '电子', '汽车', '通信', '计算机', '基础化工', '电力设备及新能源', '国防军工', '家电', '轻工制造']
pt_factor_std = std_factor(start_date, end_date)
path_pt_factor_std = os.path.join(os.getcwd(), 'factor_std_complete.csv')
pt_factor_std.to_csv(path_pt_factor_std, index=False, encoding='utf_8_sig')
