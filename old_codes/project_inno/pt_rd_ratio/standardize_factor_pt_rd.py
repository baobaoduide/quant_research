import os
import numpy as np
import pandas as pd
import statsmodels.api as sm
from old_codes.load_data.rawdata_util import load_mv_data, load_all_stock_basic_info
from old_codes.calc_tools.time_util import get_change_date_day
from old_codes.load_data.load_factor import load_raw_factor_pt_rd


def prepare_factor_df(factor_df, ashare_mv, asharedescription):
    delta_date_cut = 180
    asharedescription['date_cut'] = asharedescription['ipo_date'].apply(func=lambda x: get_change_date_day(x, delta_date_cut))
    mv_industry_df = pd.merge(ashare_mv, asharedescription[['Code', 'industry', 'date_cut']], on='Code')
    mv_industry_df = mv_industry_df[mv_industry_df['CalcDate'] > mv_industry_df['date_cut']]
    mv_industry_df.drop(columns=['date_cut'], inplace=True)
    #
    factor_df_freq = pd.merge(mv_industry_df, factor_df, on=['Code', 'CalcDate'], how='left')
    factor_df_freq.rename(columns={'pt_rd_ratio': 'factor'}, inplace=True)
    factor_df_freq[['market_value', 'factor']] = factor_df_freq.groupby(by=['Code'])[['market_value', 'factor']].ffill()
    factor_df_freq.dropna(subset=['market_value'], inplace=True)
    return factor_df_freq


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
    fill_value = factor_df_industry_sub['factor'].mean()
    normalized_factor = factor_df_industry_sub['factor'].fillna(fill_value)
    normalized_factor = removeoutlier_and_normalize(normalized_factor)
    #
    normalized_mv = removeoutlier_and_normalize(np.log(factor_df_industry_sub['market_value']))
    x = sm.add_constant(normalized_mv)
    results = sm.OLS(normalized_factor, x).fit()
    factor_df_industry_sub['factor_std'] = removeoutlier_and_normalize(results.resid)
    return factor_df_industry_sub


def cal_std_factor_certain_industry(factor_df_freq):
    factor_df_std = factor_df_freq.groupby(by=['industry', 'CalcDate']).apply(std_process_industry)
    factor_std = factor_df_std[['industry', 'Code', 'CalcDate', 'factor_std']]
    factor_std.dropna(subset=['factor_std'], inplace=True)
    factor_std.sort_values(by=['industry', 'Code', 'CalcDate'], inplace=True)
    factor_std.reset_index(drop=True, inplace=True)
    return factor_std


def std_factor(start_date, end_date):
    factor_df = load_raw_factor_pt_rd(start_date, end_date)
    fileds_mv = ['market_value']
    freq = 'M'
    ashare_mv = load_mv_data(start_date, end_date, fileds_mv, freq)
    fields_description = ['ipo_date', 'industry']
    asharedescription = load_all_stock_basic_info(fields_description)
    #
    factor_df_freq = prepare_factor_df(factor_df, ashare_mv, asharedescription)
    factor_df_std = cal_std_factor_certain_industry(factor_df_freq)
    return factor_df_std


start_date = '2010-01-01'
end_date = '2020-12-31'
pt_factor_std = std_factor(start_date, end_date)
path_pt_factor_std = os.path.join(os.getcwd(), 'factor_std_pt_rd.csv')
pt_factor_std.to_csv(path_pt_factor_std, index=False)
