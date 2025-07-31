import os
import numpy as np
import pandas as pd
import statsmodels.api as sm
from load_data_from_add import load_aindexmembers
from time_util import get_month_end_dates, adjust_month, get_next_month_end
import matplotlib.pyplot as plt
from matplotlib import font_manager
fontP = font_manager.FontProperties(fname="/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages/matplotlib/mpl-data/fonts/ttf/SimHei.ttf")
import platform
sys_platform = platform.platform().lower()
if 'macos' in sys_platform:
    path_data_local = r'/Users/xiaotianyu/Desktop/data/raw_data_add'
elif 'windows' in sys_platform:
    path_data_local = r'E:\data_gk_model\raw_data_add'
else:
    print('其他系统')


def load_gdp():
    path_data = os.path.join(path_data_local, 'gdp.xlsx')
    rtn = pd.read_excel(path_data, header=1).rename(columns={'指标名称': 'time', '中国:GDP:现价:当季值': 'GDP'})
    rtn['GDP'] = rtn['GDP'] * 100000000
    rtn['time'] = rtn['time'].dt.strftime('%Y-%m-%d')
    return rtn


def prepare_pe_pred_info_erp():
    gdp_data = load_gdp()
    gdp_data['GDP_TTM'] = gdp_data['GDP'].rolling(window=4, min_periods=4).sum()
    #
    path_data = r'E:\data_gk_model\raw_data_add\10年国债收益率.xlsx'
    bond_10y_ret = pd.read_excel(path_data, header=3).rename(columns={'Date': 'time', 'S0059749': 'bond_10y_ret'})
    bond_10y_ret['time'] = bond_10y_ret['time'].dt.strftime('%Y-%m-%d')
    bond_10y_ret['bond_10y_ret'] /= 100
    #
    folder = r'E:\data_gk_model'
    path_save = os.path.join(folder, 'all_stock_basic_info_month.csv')
    month_data_all = pd.read_csv(path_save, encoding='gbk').rename(columns={'总市值': 'mv', '分红市值': 'mv_div', '净利润_TTM': 'profit_ttm'})
    #
    month_data_all_sum = month_data_all.groupby(by=['time'], as_index=False)[['mv', 'share_total', 'profit_ttm', 'net_asset', 'tot_liab']].sum()
    #
    data_all = pd.merge(month_data_all_sum, gdp_data[['time', 'GDP_TTM']], on='time', how='left')
    data_all['GDP_TTM'].fillna(method='ffill', inplace=True)
    data_all = pd.merge(data_all, bond_10y_ret, on='time', how='left')
    data_all['bond_10y_ret'].fillna(method='ffill', inplace=True)
    #
    data_all = data_all[(data_all['time'] >= '2002-01-31') & (data_all['time'] <= '2023-07-31')]
    data_all.isna().sum()
    #
    data_all['ERP'] = data_all['profit_ttm'] / data_all['bond_10y_ret'] / data_all['mv']
    data_all['资本化率'] = data_all['profit_ttm'] / data_all['GDP_TTM']
    data_all['债务压力'] = data_all['tot_liab'] * data_all['bond_10y_ret'] / data_all['GDP_TTM']
    data_all['风险偏好'] = data_all['mv'] / data_all['tot_liab']
    # data_all.to_excel(r'E:\data_gk_model\result_predict_pe\盈利增长预测.xlsx', index=False)
    #
    data_all = pd.read_excel(r"/Users/xiaotianyu/Desktop/data/result_predict_pe/盈利增长预测.xlsx")
    data_all = data_all[data_all["time"] <= "2023-03-31"]
    data_all["time"] = pd.to_datetime(data_all["time"])
    # 资本化率
    plot_col = "风险偏好"  # 资本化率 债务压力 风险偏好
    #
    fig, ax1 = plt.subplots(figsize=(12, 6))
    color = "tab:blue"
    ax1.set_ylabel("ERP", color=color)
    ax1.plot(data_all["time"], data_all["ERP"], color=color)
    ax1.tick_params(axis="y", labelcolor=color)
    ax2 = ax1.twinx()
    color = "tab:red"
    ax2.set_ylabel(plot_col, color=color, fontproperties=fontP)
    ax2.plot(data_all["time"], data_all[plot_col], color=color)
    ax2.fill_between(data_all["time"], data_all[plot_col], color=color, alpha=0.3)  # 绘制面积图
    ax2.tick_params(axis="y", labelcolor=color)
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.show()
    return data_all


def prepare_pred_data(code_name='沪深300'):
    folder = r"/Users/xiaotianyu/Desktop/data"
    # folder = r"E:\data_gk_model"
    path_save = os.path.join(folder, "all_stock_basic_info_month.csv")
    month_data = pd.read_csv(path_save, encoding="gbk")
    month_data['分红市值'].fillna(0, inplace=True)
    month_data.sort_values(by=['code', 'time'], inplace=True)
    month_data.reset_index(drop=True, inplace=True)
    month_data['分红市值_TTM'] = month_data.groupby(by=['code'])['分红市值'].rolling(window=12,
                                                                                     min_periods=12).sum().reset_index(
        drop=True)
    month_data[['净利润_TTM', 'tot_assets', 'tot_liab', 'net_asset']] = month_data.groupby(by=['code'])[['净利润_TTM', 'tot_assets', 'tot_liab', 'net_asset']].fillna(method='ffill')
    month_data[['总市值_0', '净利润_TTM_0']] = month_data.groupby(by=['code'])[['总市值', '净利润_TTM']].shift(12)
    #
    index_dict = {
        "沪深300": "000300.SH",
        "中证500": "000905.SH",
        "上证50": "000016.SH",
        "创业板指": "399006.SZ",
        "创业板综": "399102.SZ",
        "红利指数": "000015.SH",
    }
    # folder2 = os.path.join(folder, 'raw_data_add')
    # path_temp = os.path.join(folder2, "中国A股指数成份股[AIndexMembers].csv")
    # base_info = pd.read_csv(path_temp)
    # ind_index_dict = base_info.set_index("name").to_dict()["code"]
    # ind_index_dict.update(index_dict)
    index_code = index_dict[code_name]
    if code_name in index_dict.keys():
        aindex_data = load_aindexmembers(index_code)
    # else:
    #     aindex_data = load_aindexmemberscitics(index_code)
    aindex_data["date_out"].fillna("2023-08-18", inplace=True)
    result_list = []
    dates = get_month_end_dates('2005-04-01', '2023-08-01')
    for date in dates:
        aindex_mem = aindex_data[
            (aindex_data["date_in"] <= date + "-01-01")
            & (aindex_data["date_out"] > date + "-01-01")
            ]
        print("\n", date, ", ", len(aindex_mem))
        code_list = aindex_mem["code"].to_list()
        data_code = month_data[
            (month_data["code"].isin(code_list)) & (month_data["time"] == date)
            ]
        error_check = data_code.isna().sum()
        print(error_check)
        data_df = data_code[
            ['总市值', '总市值_0', '分红市值', '分红市值_TTM', '净利润_TTM', '净利润_TTM_0', 'share_total',
             'tot_assets',
             'tot_liab', 'net_asset']]
        result = data_df.sum(skipna=True).rename(date)
        result_list.append(result)
    result_df = pd.concat(result_list, axis=1).T
    result_df = result_df.rename_axis("time", axis="index")
    result_df["PE_TTM"] = result_df["总市值"] / result_df["净利润_TTM"]
    result_df["PE_TTM"] = result_df["PE_TTM"].apply(
        func=lambda x: None if x <= 0 else x
    )
    result_df["PE_TTM_0"] = result_df["总市值_0"] / result_df["净利润_TTM_0"]
    result_df["实际收益"] = np.log(
        (result_df["总市值"] + result_df["分红市值_TTM"]) / result_df["总市值_0"]
    )
    result_df["股息驱动"] = np.log(result_df["分红市值_TTM"] / result_df["总市值"] + 1)
    result_df["业绩驱动"] = np.log(result_df["净利润_TTM"] / result_df["净利润_TTM_0"])
    result_df["估值驱动"] = np.log(result_df["PE_TTM"] / result_df["PE_TTM_0"])
    #
    # 业绩驱动修正项
    for year in result_df.index:
        profit_ = result_df["净利润_TTM"][year]
        profit_0 = result_df["净利润_TTM_0"][year]
        ret_actual = result_df["实际收益"][year]
        div_ = result_df["股息驱动"][year]
        if (profit_ < 0) & (profit_0 > 0):
            profit_ret_adj = (profit_ - profit_0) / profit_0
            result_df["业绩驱动"][year] = profit_ret_adj
            result_df["估值驱动"][year] = ret_actual - div_ - profit_ret_adj
        elif (profit_ > 0) & (profit_0 < 0):
            result_df["业绩驱动"][year] = (ret_actual - div_) / 2
            result_df["估值驱动"][year] = (ret_actual - div_) / 2
        elif (profit_ < 0) & (profit_0 < 0):
            profit_ret_adj = np.log(profit_0 / profit_)
            result_df["业绩驱动"][year] = profit_ret_adj
            result_df["估值驱动"][year] = ret_actual - div_ - profit_ret_adj
    #
    result_df["模型估计收益"] = result_df["股息驱动"] + result_df["业绩驱动"] + result_df["估值驱动"]
    #
    gdp_data = load_gdp()
    gdp_data["GDP_TTM"] = gdp_data["GDP"].rolling(window=4, min_periods=4).sum()
    #
    path_data = os.path.join(path_data_local, '10年国债收益率.xlsx')
    bond_10y_ret = pd.read_excel(path_data, header=3).rename(
        columns={"Date": "time", "S0059749": "bond_10y_ret"}
    )
    bond_10y_ret["time"] = bond_10y_ret["time"].dt.strftime("%Y-%m-%d")
    bond_10y_ret["bond_10y_ret"] /= 100
    #
    result_all = result_df.reset_index()
    result_all = pd.merge(result_all, gdp_data[['time', 'GDP_TTM']], on='time', how='left')
    result_all = pd.merge(result_all, bond_10y_ret, on='time', how='left')
    result_all["GDP_TTM"].fillna(method="ffill", inplace=True)
    result_all["bond_10y_ret"].fillna(method="ffill", inplace=True)
    #
    # 计算累计收益
    result_all[['累计收益', '股息贡献', '盈利贡献', '估值贡献']] = result_all[
        ['实际收益', '股息驱动', '业绩驱动', '估值驱动']].cumsum()
    #
    if code_name == '沪深300':
        path_save = r'E:\data_gk_model\result_predict_pe\沪深300\predict_pe_info2.xlsx'
        path_save = r'/Users/xiaotianyu/Desktop/data/result_predict_pe/沪深300/predict_pe_info2.xlsx'
    elif code_name == '中证500':
        path_save = r'E:\data_gk_model\result_predict_pe\中证500\predict_pe_info2.xlsx'
        path_save = r'/Users/xiaotianyu/Desktop/data/result_predict_pe/中证500/predict_pe_info2.xlsx'
    result_all.to_excel(path_save, index=False)
    pass


def predict_hs300_ols():
    path_save = r'E:\data_gk_model\result_predict_pe\沪深300\predict_pe_info.xlsx'
    path_save = r'/Users/xiaotianyu/Desktop/data/result_predict_pe/沪深300/predict_pe_info2.xlsx'
    result_all = pd.read_excel(path_save)
    # 计算ERP
    # 未修正的预测模型
    result_all['ERP'] = 1 / result_all['PE_TTM'] / result_all['bond_10y_ret']
    result_all['delta_pe'] = np.log(result_all['PE_TTM'].shift(-12) / result_all['PE_TTM'])
    #
    result_for_reg = result_all[['time', 'ERP', 'delta_pe']]
    loop_months = get_month_end_dates('2013-12-31', '2022-07-31')
    predict_list = []
    for i, month in enumerate(loop_months):
        train_df = result_for_reg[result_for_reg['time'] <= month]
        X = sm.add_constant(train_df['ERP'])
        y = train_df['delta_pe']
        model = sm.OLS(y, X)
        results = model.fit()
        print(month)
        print(results.summary())
        time_next = get_next_month_end(month)
        if i == 0:
            X_test = result_for_reg.loc[result_for_reg['time'] <= time_next, 'ERP']
            y_pred = results.predict(sm.add_constant(X_test)).to_list()
        else:
            X_test = result_for_reg.loc[result_for_reg['time'] == time_next, 'ERP'].values
            y_pred_i = results.predict([[1, X_test[0]]])
            y_pred.append(y_pred_i[0])
    X_test_e = result_for_reg.loc[result_for_reg['time'] > time_next, 'ERP']
    y_pred_end = results.predict(sm.add_constant(X_test_e)).to_list()
    result_for_reg['pred_delta_pe'] = y_pred + y_pred_end
    result_for_reg = result_for_reg[result_for_reg['time'] <= '2023-08-31']
    result_for_reg.to_excel(
        r"/Users/xiaotianyu/Desktop/data/result_predict_pe/沪深300/沪深300估值回归预测数据2.xlsx", index=False
    )
    result_for_reg.to_excel(r'E:\data_gk_model\result_predict_pe\hs300\沪深300估值回归预测数据2.xlsx', index=False)
    pass


def predict_zz500_ols():
    path_save = r'E:\data_gk_model\result_predict_pe\中证500\predict_pe_info.xlsx'
    result_all = pd.read_excel(path_save)
    # 计算ERP
    # 未修正的预测模型
    result_all['ERP'] = 1 / result_all['PE_TTM'] / result_all['bond_10y_ret']
    result_all['delta_pe'] = np.log(result_all['PE_TTM'].shift(-12) / result_all['PE_TTM'])
    #
    result_for_reg = result_all[['time', 'ERP', 'delta_pe']]
    result_for_reg = result_for_reg[result_for_reg['time'] >= '2007-01-31']
    loop_months = get_month_end_dates('2013-12-31', '2022-07-31')
    predict_list = []
    for i, month in enumerate(loop_months):
        train_df = result_for_reg[result_for_reg['time'] <= month]
        X = sm.add_constant(train_df['ERP'])
        y = train_df['delta_pe']
        model = sm.OLS(y, X)
        results = model.fit()
        print(results.summary())
        time_next = get_next_month_end(month)
        if i == 0:
            X_test = result_for_reg.loc[result_for_reg['time'] <= time_next, 'ERP']
            y_pred = results.predict(sm.add_constant(X_test)).to_list()
        else:
            X_test = result_for_reg.loc[result_for_reg['time'] == time_next, 'ERP'].values
            y_pred_i = results.predict([[1, X_test[0]]])
            y_pred.append(y_pred_i[0])
    X_test_e = result_for_reg.loc[result_for_reg['time'] > time_next, 'ERP']
    y_pred_end = results.predict(sm.add_constant(X_test_e)).to_list()
    result_for_reg['pred_delta_pe'] = y_pred + y_pred_end
    result_for_reg = result_for_reg[result_for_reg['time'] <= '2023-03-31']
    result_for_reg.to_excel(r'E:\data_gk_model\result_predict_pe\中证500\中证500估值回归预测数据.xlsx', index=False)
    pass


def predict_zz500_ols_adj():
    path_save = r'E:\data_gk_model\result_predict_pe\中证500\predict_pe_info_中证500.xlsx'
    path_save = r'/Users/xiaotianyu/Desktop/data/result_predict_pe/中证500/predict_pe_info2.xlsx'
    result_all = pd.read_excel(path_save)
    # result_all = result_all[['time', 'PE_TTM', '原始ERP', 'ERP_AA债收益率', 'ERP_AA-债收益率']]
    # 计算ERP
    # 未修正的预测模型
    result_all['原始ERP'] = 1 / result_all['PE_TTM'] / result_all['bond_10y_ret']
    result_all['delta_pe'] = np.log(result_all['PE_TTM'].shift(-12) / result_all['PE_TTM'])
    #
    result_for_reg = result_all[['time', '原始ERP', 'delta_pe']]
    result_for_reg = result_for_reg[result_for_reg['time'] >= '2008-01-31']
    loop_months = get_month_end_dates('2013-12-31', '2022-07-31')
    predict_list = []
    for i, month in enumerate(loop_months):
        train_df = result_for_reg[result_for_reg['time'] <= month]
        X = sm.add_constant(train_df['原始ERP'])
        y = train_df['delta_pe']
        model = sm.OLS(y, X)
        results = model.fit()
        print(results.summary())
        time_next = get_next_month_end(month)
        if i == 0:
            X_test = result_for_reg.loc[result_for_reg['time'] <= time_next, '原始ERP']
            y_pred = results.predict(sm.add_constant(X_test)).to_list()
        else:
            X_test = result_for_reg.loc[result_for_reg['time'] == time_next, '原始ERP'].values
            y_pred_i = results.predict([[1, X_test[0]]])
            y_pred.append(y_pred_i[0])
    X_test_e = result_for_reg.loc[result_for_reg['time'] > time_next, '原始ERP']
    y_pred_end = results.predict(sm.add_constant(X_test_e)).to_list()
    result_for_reg['pred_delta_pe'] = y_pred + y_pred_end
    result_for_reg = result_for_reg[result_for_reg['time'] <= '2023-07-31']
    result_for_reg.to_excel(r'/Users/xiaotianyu/Desktop/data/result_predict_pe/中证500/中证500估值回归预测数据2.xlsx', index=False)
    # result_for_reg.to_excel(r'E:\data_gk_model\result_predict_pe\中证500\中证500估值回归预测数据_adj2.xlsx', index=False)
    pass


if __name__ == '__main__':
    # prepare_pred_data(code_name='沪深300')
    #
    predict_hs300_ols()
    # predict_zz500_ols_adj()
