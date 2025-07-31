import os
import pandas as pd

from old_codes.about_gk.time_util import adjust_month, get_month_end_dates, get_month_end_date
from load_data_from_data_base import load_asharebalancesheet
from load_data_from_add import load_asharedescription, load_asharedividend, load_ashareincome, load_AShareEODDerivativeIndicator, load_asharebalancesheet2
import warnings
warnings.filterwarnings("ignore")
import platform
sys_platform = platform.platform().lower()
if 'macos' in sys_platform:
    folder = r'/Volumes/Intern/data_gk_model'
    folder_save = r'/Users/xiaotianyu/Desktop/data/result_202308'
elif 'windows' in sys_platform:
    folder = r'E:\data_gk_model'
    folder_save = r'E:\data_gk_model\result_202308'
else:
    print('其他系统')


def prepare_profit_ttm_monthly(code, income_all):
    if 'code' in income_all.columns:
        income = income_all[income_all['code'] == code]
    else:
        income = income_all
    income.sort_values(by=['date', 'report_period'], inplace=True)
    if len(income) > 0:
        month_list = get_month_end_dates(income['date'].min(), adjust_month(income['date'].max(), 1))
        profit_list = []
        for date in month_list:
            data_use = income[income['date'] <= date]
            cut_date = str(int(date[:4])-1) + date[5:7] + date[8:]
            latest_re_t = data_use['report_period'].max()
            last_latest_t = str(int(latest_re_t[:4])-1) + latest_re_t[4:]
            last_end = str(int(latest_re_t[:4])-1) + '1231'
            if latest_re_t < cut_date:
                profit_ttm = None
            elif latest_re_t[4:] == '1231':
                profit_ttm = data_use.loc[data_use['report_period'] == latest_re_t, 'profit'].values[0]
            elif set([latest_re_t, last_latest_t, last_end]).issubset(set(data_use['report_period'])):
                profit_last = data_use.loc[data_use['report_period'] == latest_re_t, 'profit'].values[0]
                profit_last_y = data_use.loc[data_use['report_period'] == last_end, 'profit'].values[0]
                profit_last_yl = data_use.loc[data_use['report_period'] == last_latest_t, 'profit'].values[0]
                profit_ttm = profit_last + profit_last_y - profit_last_yl
            else:
                profit_ttm = None
            profit_list.append(profit_ttm)
        profit_df = pd.Series(profit_list, index=month_list, name=code)
    else:
        profit_df = pd.Series(name=code)
    return profit_df


def pre_ttm_profit_m(start_date, end_date):
    codes = load_asharedescription()['code']
    income_all = load_ashareincome(codes, start_date, end_date)
    # prepare_profit_ttm_monthly('000406.SZ', income_all)
    df_list = []
    for i, code in enumerate(codes):
        print(i, code)
        profit_df = prepare_profit_ttm_monthly(code, income_all)
        df_list.append(profit_df)
    df_list_df = pd.concat(df_list, axis=1)
    return df_list_df


def prepare_balance_monthly(code, balance_df):
    if 'code' in balance_df.columns:
        balance = balance_df[balance_df['code'] == code]
    else:
        balance = balance_df
    balance.sort_values(by=['date', 'report_period'], inplace=True)
    if len(balance) > 0:
        month_list = get_month_end_dates(balance['date'].min(), adjust_month(balance['date'].max(), 1))
        if month_list == []:
            month_list = [get_month_end_date(balance['date'].max())]
        bal_list = []
        for date in month_list:
            data_use = balance[balance['date'] <= date]
            cut_date = str(int(date[:4])-1) + date[5:7] + date[8:]
            latest_re_t = data_use['report_period'].max()
            if latest_re_t < cut_date:
                info_latest = None
            else:
                info_latest = data_use.loc[data_use['report_period'] == latest_re_t, ['tot_assets', 'tot_liab', 'net_asset']]
                info_latest['time'] = date
            bal_list.append(info_latest)
        bal_df = pd.concat(bal_list)
    # elif len(balance) == 1:
    #     bal_df = balance[['tot_assets', 'tot_liab', 'net_asset']]
    #     bal_df['time'] = get_month_end_date(balance['date'].values[0])
    else:
        bal_df = pd.DataFrame()
    return bal_df


def pre_balance_info_m(start_date, end_date):
    codes = load_asharedescription()['code']
    balance1 = load_asharebalancesheet(codes, '1990-01-01', '2022-05-10')
    balance2 = load_asharebalancesheet2(codes, '2022-05-11', '2023-12-31')
    balance_all = pd.concat([balance1, balance2], ignore_index=True)
    balance_all = balance_all[(balance_all['date'] >= start_date) & (balance_all['date'] <= end_date)]
    balance_all.sort_values(by=['code', 'date'], inplace=True)
    df_list = []
    for i, code in enumerate(codes):
        print(i, code)
        balance_df = prepare_balance_monthly(code, balance_all)
        balance_df['code'] = code
        df_list.append(balance_df)
    df_list_df = pd.concat(df_list)
    df_list_df = df_list_df[['code', 'time', 'tot_assets', 'tot_liab', 'net_asset']]
    return df_list_df


def prepare_all_stock_month_info():
    base_info = load_asharedescription()
    data_mv = load_AShareEODDerivativeIndicator('1990-01-01', '2023-12-31').rename(columns={'mv': '总市值'})
    data_mv['time'] = data_mv['date'].apply(get_month_end_date)
    data_mv.sort_values(by=['code', 'date'], inplace=True)
    data_mv_month = data_mv.groupby(by=['code', 'time'], as_index=False)[['总市值', 'share_total']].last()
    #
    data_div = load_asharedividend(base_info['code'].to_list(), '1990-01-01', '2023-12-31')
    data_div['分红市值'] = data_div['cash_div_ps_pre'] * data_div['base_share']
    data_div['time'] = data_div['date'].apply(get_month_end_date)
    data_div_month = data_div.groupby(by=['code', 'time'], as_index=False)['分红市值'].sum()
    #
    path_data = os.path.join(folder, 'profit_ttm_month_199012to202307.csv')
    income = pd.read_csv(path_data, index_col=[0], header=0)
    cols_to_drop = [col for col in income.columns if len(col) != 9]
    income = income.drop(columns=cols_to_drop)
    income = income.stack().reset_index().rename(columns={'level_0': 'time', 'level_1': 'code', 0: '净利润_TTM'})
    income = income[['code', 'time', '净利润_TTM']].sort_values(by=['code', 'time']).reset_index(drop=True)
    #
    path_save = os.path.join(folder, 'balance_month_199012to202307.csv')
    balance = pd.read_csv(path_save)
    #
    month_data = pd.merge(data_mv_month, data_div_month, on=['code', 'time'], how='outer')
    month_data = pd.merge(month_data, income, on=['code', 'time'], how='outer')
    month_data = pd.merge(month_data, balance, on=['code', 'time'], how='outer')
    path_save = os.path.join(folder, 'all_stock_basic_info_month.csv')
    month_data.to_csv(path_save, index=False, encoding='gbk')
    pass


if __name__ == '__main__':
    # ana_data_use = pd.read_csv(r'E:\data_gk_model\ana_fore_np_month_test.csv')
    # result = prepare_ana_data()
    # result = result.reset_index()
    # result = result[['Code', 'time', 'con_forecast_np']].rename(columns={'Code': 'code'})
    # result.to_csv(r'E:\data_gk_model\ana_fore_np_month_test.csv', index=False)
    #
    # codes = load_asharedescription()['code']
    # income_all = load_ashareincome(codes, '2008-01-01', '2021-12-31')
    # profit_df = prepare_profit_ttm_yearly('600631.SH', income_all)
    #
    # result = pre_ttm_profit_m('2006-12-31', '2023-12-31')
    # result_save = result.sort_index()
    # path_save = os.path.join(folder, 'profit_ttm_month_200701to202307.csv')
    # result.to_csv(path_save)
    #
    # result2 = pre_balance_info_m('2006-12-31', '2023-12-31')
    # path_save = os.path.join(folder, 'balance_month_200701to202307.csv')
    # result.to_csv(path_save, index=False)
    #
    # prepare_all_stock_month_info()
    #
    result = pre_ttm_profit_m('2020-12-31', '2023-12-31')
    # result_save = result.sort_index()
    # path_save = os.path.join(folder, 'profit_ttm_month_199012to200912.csv')
    # result.to_csv(path_save)
    # result = pd.read_csv(r'E:\data_gk_model\profit_ttm_month_199012to200912.csv', index_col=0)
    # result2 = pd.read_csv(r'E:\data_gk_model\profit_ttm_month_200701to202307.csv', index_col=0)
    # df1 = result.loc[:'2008-12-31']
    # df2 = result2.loc['2009-01-31':]
    # result_all = pd.concat([df1, df2])
    # result_all.to_csv(r'E:\data_gk_model\profit_ttm_month_199012to202307.csv')
    #
    # result2 = pre_balance_info_m('1990-12-31', '2023-12-31')
    # path_save = os.path.join(folder, 'balance_month_199012to202307.csv')
    # result2.to_csv(path_save, index=False)
    #
    # prepare_all_stock_month_info()
