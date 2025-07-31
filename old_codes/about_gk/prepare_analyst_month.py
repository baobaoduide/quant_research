import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from old_codes.about_gk.load_data_from_add import load_data_from_gogoal2
from old_codes.about_gk.load_data_from_data_base import load_data_from_gogoal
from old_codes.about_gk.time_util import adjust_month, get_month_end_dates
from load_data_from_add import load_asharedescription, load_ashareincome


def prepare_code_ana(code_df):
    month_list = get_month_end_dates(code_df['create_date'].min(), adjust_month(code_df['create_date'].max(), 1))
    con_fore_data = []
    for date in month_list:
        date_0 = adjust_month(date, -3)
        df_use = code_df[(code_df['create_date'] >= date_0) & (code_df['create_date'] <= date)]
        report_period_max = df_use['report_year'].max()
        df_use_adj = df_use[df_use['report_year'] == report_period_max]
        con_np = df_use_adj['forecast_np'].mean()
        con_fore_data.append(con_np)
    month_df = pd.DataFrame({'time': month_list, 'con_forecast_np': con_fore_data})
    return month_df


def prepare_ana_data():
    flds_use = ["report_type", "author_name", "stock_name", "forecast_np", "forecast_eps", "forecast_dps",
                "forecast_pe", "forecast_roe"]
    ana_data = load_data_from_gogoal("2009-01-01", "2021-12-31", flds_use, "csv")
    ana_data2 = load_data_from_gogoal2("2022-01-01", "2023-08-21", flds_use, "csv")
    ana_data_all = pd.concat([ana_data, ana_data2], ignore_index=True)
    ana_data_use = ana_data_all[ana_data_all['report_quarter'] == 4]
    ana_data_use = ana_data_use[~ana_data_use['forecast_np'].isna()]
    ana_data_use = ana_data_use[['Code', 'organ_name', 'author_name', 'create_date', 'report_year', 'forecast_np']]
    ana_data_use.sort_values(by=['Code', 'create_date', 'organ_name', 'author_name', 'report_year'], inplace=True)
    ana_data_use = ana_data_use.groupby(by=['Code', 'create_date', 'organ_name', 'author_name'], as_index=False).first(2)
    # code_list = ana_data_use['Code'].drop_duplicates().to_list()
    # code = code_list[0]
    # data_code = ana_data_use[ana_data_use['Code'] == code]
    # month_list = get_month_end_dates(data_code['create_date'].min(), adjust_month(data_code['create_date'].max(), 1))
    # con_fore_data = []
    # for date in month_list:
    #     date_0 = adjust_month(date, -3)
    #     df_use = data_code[(data_code['create_date'] >= date_0) & (data_code['create_date'] <= date)]
    #     report_period_max = df_use['report_year'].max()
    #     df_use_adj = df_use[df_use['report_year'] == report_period_max]
    #     con_np = df_use_adj['forecast_np'].mean()
    #     con_fore_data.append(con_np)
    # month_df = pd.DataFrame({'time': month_list, 'con_forecast_np': con_fore_data})
    #
    # for code in ana_data_use['Code'].drop_duplicates().to_list():
    #     data_code = ana_data_use[ana_data_use['Code'] == code]
    #     result = prepare_code_ana(data_code)
    ana_data_month = ana_data_use.groupby(by=['Code']).apply(prepare_code_ana)
    ana_data_month.to_csv(r'E:\data_gk_model\ana_fore_np_month_test.csv')
    return ana_data_month


def shift_months(input_date, n):
    date_format = '%Y%m%d'
    input_date = datetime.strptime(input_date, date_format)
    month_offset = relativedelta(months=n)
    shifted_date = input_date + month_offset
    shifted_date_str = shifted_date.strftime(date_format)
    return shifted_date_str


def prepare_profit_q_monthly(code, income_all):
    if 'code' in income_all.columns:
        income = income_all[income_all['code'] == code]
    else:
        income = income_all
    income.sort_values(by=['date', 'report_period'], inplace=True)
    if len(income) > 0:
        month_list = get_month_end_dates(income['date'].min(), adjust_month(income['date'].max(), 1))
        profit_list = []
        profit_quarter_list = []
        profit_quarter_ind_list = []
        for date in month_list:
            data_use = income[income['date'] <= date]
            latest_re_t = data_use['report_period'].max()
            if latest_re_t in data_use['report_period'].to_list():
                profit_last = data_use.loc[data_use['report_period'] == latest_re_t, 'profit'].values[0]
                profit_q = profit_last
                quarter = latest_re_t
                quarter_ind = int(int(latest_re_t[4:6])/3)
            else:
                profit_q = None
                quarter = None
                quarter_ind = None
            profit_list.append(profit_q)
            profit_quarter_list.append(quarter)
            profit_quarter_ind_list.append(quarter_ind)
        profit_df = pd.DataFrame({'code': code, 'time': month_list, 'profit_q': profit_list, 'quarter': profit_quarter_list, 'quarter_ind': profit_quarter_ind_list})
    else:
        profit_df = pd.DataFrame()
    return profit_df


def pre_profit_q_m(start_date, end_date):
    codes = load_asharedescription()['code']
    income_all = load_ashareincome(codes, start_date, end_date)
    # prepare_profit_ttm_monthly('000406.SZ', income_all)
    df_list = []
    for i, code in enumerate(codes):
        print(i, code)
        profit_df = prepare_profit_q_monthly(code, income_all)
        df_list.append(profit_df)
    # df_list_df = pd.concat(df_list)
    return df_list


def prepare_ana_data2():
    flds_use = ["report_type", "author_name", "stock_name", "forecast_np", "forecast_eps", "forecast_dps",
                "forecast_pe", "forecast_roe"]
    ana_data = load_data_from_gogoal("2009-01-01", "2021-12-31", flds_use, "csv")
    ana_data2 = load_data_from_gogoal2("2022-01-01", "2023-08-21", flds_use, "csv")
    ana_data_all = pd.concat([ana_data, ana_data2], ignore_index=True)
    ana_data_use = ana_data_all[ana_data_all['report_quarter'] == 4]
    ana_data_use = ana_data_use[~ana_data_use['forecast_np'].isna()]
    ana_data_use = ana_data_use[['Code', 'organ_name', 'author_name', 'create_date', 'report_year', 'forecast_np']]
    ana_data_use.sort_values(by=['Code', 'create_date', 'organ_name', 'author_name', 'report_year'], inplace=True)
    ana_data_use = ana_data_use.groupby(by=['Code', 'create_date', 'organ_name', 'author_name'], as_index=False).first(2)
    ana_data_use.to_csv(r'E:\data_gk_model\ana_forecast_raw2.csv')
    pass


def adjust_fore(code, time, report_year, profit, term_id, ana_data_use):
    print(time)
    time_3m = adjust_month(time, -3)
    ana_data = ana_data_use[(ana_data_use['Code'] == code) & (ana_data_use['create_date'] <= time) & (
                ana_data_use['create_date'] >= time_3m)]
    con_ana = ana_data.groupby(by=['report_year'])['forecast_np'].mean()
    if term_id == 4:
        if report_year in con_ana.index:
            result = con_ana[report_year]
        else:
            result = None
    else:
        if (report_year in con_ana.index) & (report_year + 1 in con_ana.index):
            result = con_ana[report_year] - profit + con_ana[report_year + 1] / 4 * (4 - term_id)
        else:
            result = None
    return result


def cal_ana_data_new():
    profit_df = pd.read_csv(r'E:\data_gk_model\profit_month_raw.csv')
    ana_data_use = pd.read_csv(r'E:\data_gk_model\ana_forecast_raw2.csv')
    ana_data_use.sort_values(by=['Code', 'create_date', 'organ_name', 'author_name', 'report_year'], inplace=True)
    profit_df = profit_df[profit_df['time'] >= '2009-01-01']
    code_list = profit_df['code'].drop_duplicates().sort_values().to_list()
    result_df = []
    profit_df['ana_adj'] = profit_df.apply(adjust_fore)
    for code in code_list:
        print(code)
        df_code = profit_df[profit_df['code'] == code]
        for time in df_code['time']:
            print(time)
            df_time = df_code[df_code['time'] == time]
            report_year = int(df_time['quarter'].values[0]/10000)
            profit = df_time['profit_q'].values[0]
            term_id = df_time['quarter_ind'].values[0]
            time_3m = adjust_month(time, -3)
            ana_data = ana_data_use[(ana_data_use['Code'] == code) & (ana_data_use['create_date'] <= time) & (ana_data_use['create_date'] >= time_3m)]
            con_ana = ana_data.groupby(by=['report_year'])['forecast_np'].mean()
            if term_id == 4:
                if report_year in con_ana.index:
                    result = con_ana[report_year]
                else:
                    result = None
            else:
                if (report_year in con_ana.index) & (report_year+1 in con_ana.index):
                    result = con_ana[report_year] - profit + con_ana[report_year+1]/4*(4-term_id)
                else:
                    result = None
            result_df.append(result)
    profit_df['forecast_np_adj'] = result_df
    return profit_df


if __name__ == '__main__':
    # prepare_ana_data2()
    ana_data_new = cal_ana_data_new()
    # result = pre_profit_q_m('1990-12-31', '2023-12-31')
    # df_list_df = pd.concat(result)
    # df_list_df.to_csv(r'E:\data_gk_model\profit_month_raw.csv', index=False)
    # result = prepare_ana_data()
    # result = result.reset_index()
    # result = result[['Code', 'time', 'con_forecast_np']].rename(columns={'Code': 'code'})
