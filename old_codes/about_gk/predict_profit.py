import os
import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
from matplotlib import font_manager
fontP = font_manager.FontProperties(fname="/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages/matplotlib/mpl-data/fonts/ttf/SimHei.ttf")
from load_data_from_data_base import load_data_from_gogoal
from load_data_from_add import load_data_from_gogoal2, load_ashareincome, load_asharedescription, load_aindexmembers, load_aindexmemberscitics
folder = r'E:\data_gk_model\result_predict_np\predit_profit_down_up_month'


def predict_up_down_hs300(train_d_s, train_d_e, predict_d):
    path_data = r'/Users/xiaotianyu/Desktop/data/盈利预测_自上而下_log.xlsx'
    # path_data = r'E:\data_gk_model\盈利预测_自上而下_log.xlsx'
    file = pd.ExcelFile(path_data)
    hs300 = pd.read_excel(file, '盈利预测_沪深300')
    hs300['y'] = hs300['沪深300净利润增速'].shift(-12) - hs300['沪深300净利润增速']
    hs300['x1'] = hs300['ROE*(1-派息率)'] - hs300['沪深300净利润增速']
    hs300['x2'] = hs300['中国信贷脉冲']
    hs300 = hs300[hs300['时间'] >= train_d_s]
    # hs300_use = hs300.dropna(subset=['y'])
    X = sm.add_constant(hs300[['x1', 'x2']])
    y = hs300['y']
    split_date = train_d_e
    train_idx = hs300['时间'] < split_date
    test_idx = hs300['时间'] >= split_date
    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]
    model = sm.OLS(y_train, X_train)
    results = model.fit()
    print(results.summary())
    y_pred = results.predict(X_test)
    y_pred_all = results.predict(X)
    time = pd.to_datetime(hs300['时间']) + pd.offsets.MonthEnd(0)
    idx = hs300['时间'][hs300['时间'] == predict_d].index[0]
    y_pred_latest = y_pred_all.loc[idx] + hs300['沪深300净利润增速'].loc[idx]
    print(y_pred_latest)
    fig, ax = plt.subplots(1, figsize=(10, 6))
    plt.plot(time, y, color='blue', linewidth=1, label='沪深300盈利增速变化')
    plt.plot(time, y_pred_all, color='orange', linewidth=1, label='预测值')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_color('gray')
    plt.legend()
    folder = r'E:\data_gk_model\result_predict'
    path_save_fig = os.path.join(folder, '沪深300盈利增速预测_'+predict_d[:4]+'.png')
    plt.savefig(path_save_fig)
    plt.close()
    hs300['增速变化预测值'] = y_pred_all
    hs300['盈利增速预测值'] = hs300['沪深300净利润增速'] + y_pred_all
    path_save_data = os.path.join(folder, '沪深300盈利增速预测_'+predict_d[:4]+'.xlsx')
    hs300.to_excel(path_save_data, index=False)
    pass


def predict_up_down_zz500(train_d_s, train_d_e, predict_d):
    path_data = r'E:\data_gk_model\盈利预测_自上而下_log.xlsx'
    path_data = r'/Users/xiaotianyu/Desktop/data/盈利预测_自上而下_log.xlsx'
    file = pd.ExcelFile(path_data)
    raw_data = pd.read_excel(file, '盈利预测_中证500')
    raw_data['y'] = raw_data['中证500净利润增速'].shift(-12) - raw_data['中证500净利润增速']
    raw_data['x1'] = raw_data['ROE*(1-派息率)'] - raw_data['中证500净利润增速']
    raw_data['x2'] = raw_data['中国信贷脉冲']
    raw_data = raw_data[raw_data['时间'] >= train_d_s]
    # hs300_use = hs300.dropna(subset=['y'])
    X = sm.add_constant(raw_data[['x1', 'x2']])
    y = raw_data['y']
    split_date = train_d_e
    train_idx = raw_data['时间'] < split_date
    test_idx = raw_data['时间'] >= split_date
    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]
    model = sm.OLS(y_train, X_train)
    results = model.fit()
    print(results.summary())
    y_pred = results.predict(X_test)
    y_pred_all = results.predict(X)
    time = pd.to_datetime(raw_data['时间']) + pd.offsets.MonthEnd(0)
    idx = raw_data['时间'][raw_data['时间'] == predict_d].index[0]
    y_pred_latest = y_pred_all.loc[idx] + raw_data['中证500净利润增速'].loc[idx]
    print(y_pred_latest)
    fig, ax = plt.subplots(1, figsize=(10, 6))
    plt.plot(time, y, color='blue', linewidth=1, label='中证500盈利增速变化')
    plt.plot(time, y_pred_all, color='orange', linewidth=1, label='预测值')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_color('gray')
    plt.legend(prop=fontP)
    folder = r'E:\data_gk_model\result_predict'
    folder = r'/Users/xiaotianyu/Desktop/data/result_predict_e'
    path_save_fig = os.path.join(folder, '中证500盈利增速预测_'+predict_d[:4]+'.png')
    plt.savefig(path_save_fig)
    plt.close()
    raw_data['增速变化预测值'] = y_pred_all
    raw_data['盈利增速预测值'] = raw_data['中证500净利润增速'] + y_pred_all
    path_save_data = os.path.join(folder, '中证500盈利增速预测_'+predict_d[:4]+'.xlsx')
    raw_data.to_excel(path_save_data, index=False)
    pass


def check_ana_data():
    flds_use = ["report_type", "author_name", "stock_name", "forecast_np", "forecast_eps", "forecast_dps", "forecast_pe", "forecast_roe"]
    ana_data = load_data_from_gogoal("2009-01-01", "2021-12-31", flds_use, "csv")
    ana_data2 = load_data_from_gogoal2("2022-01-01", "2023-08-21", flds_use, "csv")
    ana_data_all = pd.concat([ana_data, ana_data2], ignore_index=True)
    ana_data_use = ana_data_all[ana_data_all['report_quarter'] == 4]
    ana_data_use = ana_data_use[~ana_data_use['forecast_np'].isna()]
    ana_data_use = ana_data_use[['Code', 'organ_name', 'author_name', 'create_date', 'report_year', 'forecast_np']]
    ana_data_use.sort_values(by=['Code', 'create_date', 'organ_name', 'author_name', 'report_year'], inplace=True)
    ana_data_use = ana_data_use.groupby(by=['Code', 'create_date', 'organ_name', 'author_name'], as_index=False).first()
    ana_data_use.to_csv(r'/Users/xiaotianyu/Desktop/data/raw_forecast_np/ana_forecast_np.csv', index=False)
    #
    return ana_data


def check_actual_np_data():
    codes = load_asharedescription()['code']
    income_data = load_ashareincome(codes, '2008-01-01', '2023-08-18')
    income_data.sort_values(by=['code', 'report_period', 'date'], inplace=True)
    income_data_use = income_data[income_data['report_period'].apply(func=lambda x: x[-4:] == '1231')]
    income_data_use['report_year'] = income_data_use['report_period'].apply(func=lambda x: int(x[:4]))
    income_data_use2 = income_data_use.groupby(by=['code', 'report_year'])[['profit', 'date']].first()
    # income_data_use2.to_csv(r'/Users/xiaotianyu/Desktop/data/raw_forecast_np/net_profit_yearly.csv')
    income_data_use['ann_m'] = income_data_use['date'].apply(func=lambda x: x[5:7])
    pass


def get_month_end_dates(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    month_end_dates = []
    while start_date <= end_date:
        next_month = start_date.replace(day=28) + timedelta(days=4)
        month_end_date = next_month - timedelta(days=next_month.day)

        if month_end_date <= end_date:
            month_end_dates.append(month_end_date.strftime('%Y-%m-%d'))

        start_date = month_end_date + timedelta(days=1)
    return month_end_dates


def evaluate_np_data(code_name='沪深300'):
    index_dict = {'沪深300': '000300.SH', '中证500': '000905.SH', '上证50': '000016.SH', '创业板指': '399006.SZ',
                  '创业板综': '399102.SZ', '红利指数': '000015.SH'}
    path_temp = os.path.join(r'E:\data_gk_model\citic_ind_l1_index_list.xlsx')
    base_info = pd.read_excel(path_temp)
    base_info['name'] = base_info['name'].apply(func=lambda x: x[:-4])
    ind_index_dict = base_info.set_index('name').to_dict()['code']
    ind_index_dict.update(index_dict)
    index_code = ind_index_dict[code_name]
    # income_use = pd.read_csv(r'/Users/xiaotianyu/Desktop/data/raw_forecast_np/net_profit_yearly.csv')
    # forecast_np = pd.read_csv(r'/Users/xiaotianyu/Desktop/data/raw_forecast_np/ana_forecast_np.csv')
    income_use = pd.read_csv(r'E:\data_gk_model\raw_forecast_np\net_profit_yearly.csv')
    forecast_np = pd.read_csv(r'E:\data_gk_model\raw_forecast_np\ana_forecast_np.csv')
    path_save = r'E:\data_gk_model\all_stock_basic_info.csv'
    year_data = pd.read_csv(path_save, encoding='gbk')
    year_data.rename(columns={'年份': 'report_year', '净利润_TTM': 'profit_ttm'}, inplace=True)
    # 针对沪深300预测
    forecast_np['cut1'] = forecast_np['report_year'].apply(func=lambda x: str(x) + '-01-01')
    forecast_np['cut2'] = forecast_np['report_year'].apply(func=lambda x: str(x) + '-06-30')
    forecast_np_use = forecast_np[(forecast_np['create_date'] >= forecast_np['cut1']) & (forecast_np['create_date'] <= forecast_np['cut2'])]
    con_forecast = forecast_np_use.groupby(by=['Code', 'report_year'], as_index=False)['forecast_np'].mean()
    con_forecast.rename(columns={'Code': 'code'}, inplace=True)
    income_use = income_use[income_use['report_year'] >= 2009]
    compare_df = pd.merge(con_forecast, income_use, on=['code', 'report_year'], how='outer')
    compare_df = pd.merge(compare_df, year_data[['code', 'report_year', 'profit_ttm']], on=['code', 'report_year'], how='outer')
    compare_df['forecast_np_adj'] = compare_df.apply(lambda row: row['forecast_np'] if pd.notnull(row['forecast_np']) else row['profit_ttm'], axis=1)
    # check_df = compare_df.groupby(by=['report_year']).agg(lambda x: x.isnull().sum())
    if code_name in index_dict.keys():
        aindex_data = load_aindexmembers(index_code)
    else:
        aindex_data = load_aindexmemberscitics(index_code)
    aindex_data['date_out'].fillna('2023-08-18', inplace=True)
    result_list = []
    for year in range(2010, 2024):
        aindex_mem = aindex_data[
            (aindex_data['date_in'] <= str(year) + '-01-01') & (aindex_data['date_out'] > str(year) + '-01-01')]
        print('\n', year, ', ', len(aindex_mem))
        code_list = aindex_mem['code'].to_list()
        data_code = compare_df[(compare_df['code'].isin(code_list)) & (compare_df['report_year'] == year)]
        error_check = data_code.isna().sum()
        print(error_check)
        sum_info = data_code[['profit', 'forecast_np_adj']].sum().rename(year)
        result_list.append(sum_info)
    result_df = pd.concat(result_list, axis=1).T
    result_df['profit_yoy'] = np.log(result_df['profit'] / result_df['profit'].shift(1))
    result_df['fore_np_yoy'] = np.log(result_df['forecast_np_adj'] / result_df['forecast_np_adj'].shift(1))
    result_num = result_df.loc[2023, 'fore_np_yoy']
    result_df_use = result_df.loc[2011:2023]
    result_df_use.loc[2023, 'profit_yoy'] = None
    #
    title = '沪深300盈利增速'
    fig, ax = plt.subplots(1, figsize=(10, 6))
    plt.plot(result_df_use.index, result_df_use['profit_yoy'], color='yellow', marker='.', linewidth=1, label=code_name+'真实增速')
    plt.plot(result_df_use.index, result_df_use['fore_np_yoy'], color='blue', marker='.', linewidth=1, label=code_name+'预测增速')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_color('gray')
    # plt.title(title, fontsize=16)
    ax.set_ylabel('百分比 (%)')
    plt.legend()
    path_save_fig = os.path.join(folder, '分析师数据盈利增速预测_' + code_name + '_修正.png')
    plt.savefig(path_save_fig)
    plt.close()
    plt.show()
    # 先针对东方财富计算
    # forecast_dfcf = forecast_np[forecast_np['Code'] == '300059.SZ']
    # forecast_dfcf['cut1'] = forecast_dfcf['report_year'].apply(func=lambda x: str(x) + '-01-01')
    # forecast_dfcf['cut2'] = forecast_dfcf['report_year'].apply(func=lambda x: str(x) + '-06-30')
    # forecast_dfcf_use = forecast_dfcf[(forecast_dfcf['create_date'] >= forecast_dfcf['cut1']) & (forecast_dfcf['create_date'] <= forecast_dfcf['cut2'])]
    # con_forecast = forecast_dfcf_use.groupby(by=['report_year'], as_index=False)['forecast_np'].mean()
    # income_dfcf = income_use[income_use['code'] == '300059.SZ']
    # compare_df = pd.merge(con_forecast, income_dfcf[['report_year', 'profit']], on='report_year', how='left')
    # compare_df['forecast_np_delta'] = np.log(compare_df['forecast_np'] / compare_df['forecast_np'].shift(1))
    # compare_df['profit_delta'] = np.log(compare_df['profit'] / compare_df['profit'].shift(1))
    # compare_df.dropna(subset=['forecast_np_delta'], inplace=True)
    # compare_df.set_index('report_year', inplace=True)
    # compare_df[['forecast_np_delta', 'profit_delta']].plot()
    return result_df_use


def evaluate_np_data_month(code_name='沪深300'):
    index_dict = {'沪深300': '000300.SH', '中证500': '000905.SH', '上证50': '000016.SH', '创业板指': '399006.SZ',
                  '创业板综': '399102.SZ', '红利指数': '000015.SH'}
    path_temp = os.path.join(r'E:\data_gk_model\citic_ind_l1_index_list.xlsx')
    base_info = pd.read_excel(path_temp)
    base_info['name'] = base_info['name'].apply(func=lambda x: x[:-4])
    ind_index_dict = base_info.set_index('name').to_dict()['code']
    ind_index_dict.update(index_dict)
    index_code = ind_index_dict[code_name]
    # income_use = pd.read_csv(r'/Users/xiaotianyu/Desktop/data/raw_forecast_np/net_profit_yearly.csv')
    # forecast_np = pd.read_csv(r'/Users/xiaotianyu/Desktop/data/raw_forecast_np/ana_forecast_np.csv')
    income_use = pd.read_csv(r'E:\data_gk_model\all_stock_basic_info_month.csv', encoding='gbk')
    forecast_np = pd.read_csv(r'E:\data_gk_model\ana_fore_np_month_test.csv', encoding='gbk')
    month_data = pd.merge(income_use, forecast_np, on=['code', 'time'], how='left')
    # 针对沪深300预测
    month_data_use = month_data[(month_data['time'] >= '2009-01-01') & (month_data['time'] <= '2023-03-31')]
    # compare_df['forecast_np_adj'] = compare_df.apply(lambda row: row['forecast_np'] if pd.notnull(row['forecast_np']) else row['profit_ttm'], axis=1)
    # check_df = compare_df.groupby(by=['report_year']).agg(lambda x: x.isnull().sum())
    if code_name in index_dict.keys():
        aindex_data = load_aindexmembers(index_code)
    else:
        aindex_data = load_aindexmemberscitics(index_code)
    aindex_data['date_out'].fillna('2023-08-18', inplace=True)
    result_list = []
    month_list = month_data_use['time'].drop_duplicates().to_list()
    for time in month_list:
        aindex_mem = aindex_data[
            (aindex_data['date_in'] <= str(time) + '-01-01') & (aindex_data['date_out'] > str(time) + '-01-01')]
        print('\n', time, ', ', len(aindex_mem))
        code_list = aindex_mem['code'].to_list()
        data_code = month_data_use[(month_data_use['code'].isin(code_list)) & (month_data_use['time'] == time)]
        data_code['con_forecast_np_adj'] = data_code.apply(
            lambda row: row['con_forecast_np'] if pd.notnull(row['con_forecast_np']) else row['净利润_TTM'], axis=1)
        error_check = data_code.isna().sum()
        print(error_check)
        sum_info = data_code[['净利润_TTM', 'con_forecast_np_adj']].sum().rename(time)
        result_list.append(sum_info)
    result_df = pd.concat(result_list, axis=1).T
    result_df['profit_yoy'] = np.log(result_df['净利润_TTM'].shift(-12) / result_df['净利润_TTM'])
    result_df['fore_np_yoy'] = np.log(result_df['con_forecast_np_adj'] / result_df['净利润_TTM'])
    result_df_plot = result_df.loc['2011-01-31':'2023-03-31']
    time = pd.to_datetime(result_df_plot.index)
    #
    fig, ax = plt.subplots(1, figsize=(10, 6))
    plt.plot(time, result_df_plot['profit_yoy'], color='yellow', linewidth=1, label=code_name+'真实增速')
    plt.plot(time, result_df_plot['fore_np_yoy'], color='blue', linewidth=1, label=code_name+'预测增速')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_color('gray')
    ax.set_ylabel('百分比 (%)')
    plt.legend()
    path_save_fig = os.path.join(folder, '分析师数据盈利增速预测_' + code_name + '_月度.png')
    plt.savefig(path_save_fig)
    plt.close()
    plt.show()
    return result_df_plot


if __name__ == '__main__':
    # result = evaluate_np_data_month(code_name='沪深300')
    predict_up_down_zz500('2009-01', '2019-12', '2023-07')
    # predict_up_down_zz500('2008-06', '2017-12', '2021-07')
    # predict_up_down_hs300('2007-01', '2017-12', '2021-07')
    # predict_up_down_hs300('2009-01', '2019-12', '2023-07')
    # check_ana_data()
    # check_actual_np_data()
    broad_index_list = ['沪深300', '中证500', '上证50', '创业板指', '红利指数']
    ind_index_list = ["石油石化", "煤炭", "有色金属", "电力及公用事业", "钢铁", "基础化工", "建筑", "建材", "轻工制造",
                      "机械", "电力设备及新能源", "国防军工", "汽车", "商贸零售", "消费者服务", "家电", "纺织服装",
                      "医药", "食品饮料", "农林牧渔", "银行", "非银行金融", "房地产", "交通运输", "电子", "通信",
                      "计算机", "传媒", "综合", "综合金融"]
    all_list = broad_index_list + ind_index_list
    path_data = os.path.join(folder, '分析师数据盈利增速预测结果整合.xlsx')
    writer = pd.ExcelWriter(path_data)
    r_num = 1
    for name in all_list:
        result = evaluate_np_data(code_name=name)
        print(name)
        result.to_excel(writer, sheet_name='sheet', startrow=r_num)
        # writer.sheets["sheet"].cell(row=r_num, column=1, value=name)
        writer.sheets['sheet'].write_string(r_num-1, 0, name)
        r_num += len(result) + 3
    writer.save()
