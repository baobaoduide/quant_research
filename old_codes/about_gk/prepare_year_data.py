import os
import pandas as pd
from load_data_from_data_base import load_asharemv
from load_data_from_add import load_asharedescription, load_asharedividend, load_ashareincome, load_AShareEODDerivativeIndicator
import warnings
warnings.filterwarnings("ignore")
import platform
sys_platform = platform.platform().lower()
if 'macos' in sys_platform:
    folder = r'/Users/xiaotianyu/Desktop/data'
    folder_save = r'/Users/xiaotianyu/Desktop/data/result_202308'
elif 'windows' in sys_platform:
    folder = r'E:\data_gk_model'
    folder_save = r'E:\data_gk_model\result_202308'
else:
    print('其他系统')


def prepare_profit_ttm_yearly(code, income_all):
    if 'code' in income_all.columns:
        income = income_all[income_all['code'] == code]
    else:
        income = income_all
    income['report_year'] = income['report_period'].apply(func=lambda x: int(x[:4]))
    income['ann_year'] = income['date'].apply(func=lambda x: int(x[:4]))
    profit_list = []
    for year in range(2010, 2023):
        data_use = income[income['ann_year'] <= year]
        suppose_report_period = str(year) + '1231'
        latest_report_period = str(year) + '0930'
        last_end = str(year-1) + '1231'
        last_latest = str(year-1) + '0930'
        if suppose_report_period in data_use['report_period']:
            profit_ttm = data_use.loc[data_use['report_period'] == suppose_report_period, 'profit'].values[0]
        elif set([latest_report_period, last_end, last_latest]).issubset(set(data_use['report_period'])):
            profit_last = data_use.loc[data_use['report_period'] == latest_report_period, 'profit'].values[0]
            profit_last_y = data_use.loc[data_use['report_period'] == last_end, 'profit'].values[0]
            profit_last_yl = data_use.loc[data_use['report_period'] == last_latest, 'profit'].values[0]
            profit_ttm = profit_last + profit_last_y - profit_last_yl
        else:
            profit_ttm = None
        profit_list.append(profit_ttm)
    profit_df = pd.Series(profit_list, index=list(range(2010, 2023)), name=code)
    return profit_df


def pre_ttm_profit_y(start_d, end_d):
    codes = load_asharedescription()['code']
    income_all = load_ashareincome(codes, start_d, end_d)
    df_list = []
    for i, code in enumerate(codes):
        print(i, code)
        profit_df = prepare_profit_ttm_yearly(code, income_all)
        df_list.append(profit_df)
    df_list_df = pd.concat(df_list, axis=1)
    df_list_df = df_list_df.T
    path_save = os.path.join(folder, 'profit_ttm_year_2010to2022.csv')
    df_list_df.to_csv(path_save)
    return df_list_df


def prepare_all_stock_year_info(start_d, end_d):
    base_info = load_asharedescription()
    data_mv = load_AShareEODDerivativeIndicator(start_d, end_d)[['code', 'date', 'mv']]
    data_mv.rename(columns={'mv': '总市值'}, inplace=True)
    data_mv['年份'] = data_mv['date'].apply(func=lambda x: int(x[:4]))
    data_mv.sort_values(by=['code', 'date'], inplace=True)
    data_mv_year = data_mv.groupby(by=['code', '年份'], as_index=False)['总市值'].last()
    data_div = load_asharedividend(base_info['code'].to_list(), start_d, end_d)
    data_div['分红市值'] = data_div['cash_div_ps_pre'] * data_div['base_share']
    data_div['年份'] = data_div['date'].apply(func=lambda x: int(x[:4]))
    data_div_year = data_div.groupby(by=['code', '年份'], as_index=False)['分红市值'].sum()
    path_data = os.path.join(folder, 'profit_ttm_year_2010to2022.csv')
    income = pd.read_csv(path_data, index_col=[0], header=0)
    income = income.stack().reset_index().rename(columns={'level_0': 'code', 'level_1': '年份', 0: '净利润_TTM'})
    income['年份'] = income['年份'].astype(int)
    #
    year_data = pd.merge(data_mv_year, data_div_year, on=['code', '年份'], how='outer')
    year_data = pd.merge(year_data, income, on=['code', '年份'], how='outer')
    path_save = os.path.join(folder, 'all_stock_basic_info.csv')
    year_data.to_csv(path_save, index=False, encoding='gbk')
    pass


if __name__ == '__main__':
    # codes = load_asharedescription()['code']
    # income_all = load_ashareincome(codes, '2008-01-01', '2021-12-31')
    # profit_df = prepare_profit_ttm_yearly('600631.SH', income_all)
    #
    # pre_ttm_profit_y('2007-12-31', '2022-12-31')
    prepare_all_stock_year_info('2009-12-31', '2023-12-31')
