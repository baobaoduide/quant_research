import os
import numpy as np
import pandas as pd
from load_data_from_add import load_aindexmembers, load_aindexmemberscitics
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
if not os.path.exists(folder_save):
    os.makedirs(folder_save)


def split_process(code_name):
    path_save = os.path.join(folder, 'all_stock_basic_info.csv')
    year_data = pd.read_csv(path_save, encoding='gbk')
    year_data['总市值_0'] = year_data.groupby(by=['code'])['总市值'].shift(1)
    year_data['净利润_TTM_0'] = year_data.groupby(by=['code'])['净利润_TTM'].shift(1)
    #
    stock_dict = {'东方财富': '300059.SZ', '贵州茅台': '600519.SH', '恒瑞医药': '600276.SH'}
    index_dict = {'沪深300': '000300.SH', '中证500': '000905.SH', '上证50': '000016.SH', '创业板指': '399006.SZ', '创业板综': '399102.SZ', '红利指数': '000015.SH'}
    path_temp = os.path.join(folder, 'citic_ind_l1_index_list.xlsx')
    base_info = pd.read_excel(path_temp)
    base_info['name'] = base_info['name'].apply(func=lambda x: x[:-4])
    ind_index_dict = base_info.set_index('name').to_dict()['code']
    ind_index_dict.update(index_dict)
    if code_name in ind_index_dict.keys():
        index_code = ind_index_dict[code_name]
        if code_name in index_dict.keys():
            aindex_data = load_aindexmembers(index_code)
        else:
            aindex_data = load_aindexmemberscitics(index_code)
        aindex_data['date_out'].fillna('2023-08-18', inplace=True)
        result_list = []
        for year in range(2010, 2023):
            aindex_mem = aindex_data[(aindex_data['date_in'] <= str(year) + '-01-01') & (aindex_data['date_out'] > str(year) + '-01-01')]
            print('\n', year, ', ', len(aindex_mem))
            code_list = aindex_mem['code'].to_list()
            data_code = year_data[(year_data['code'].isin(code_list)) & (year_data['年份'] == year)]
            error_check = data_code.isna().sum()
            print(error_check)
            check1 = data_code[data_code['净利润_TTM'].isna()]
            check2 = data_code[data_code['总市值'].isna()]
            data_df = data_code[['总市值', '分红市值', '净利润_TTM', '总市值_0', '净利润_TTM_0']]
            result = data_df.sum(skipna=True).rename(year)
            result_list.append(result)
        result_df = pd.concat(result_list, axis=1).T
        result_df = result_df.rename_axis('年份', axis='index')
        result_df['PE_TTM'] = result_df['总市值'] / result_df['净利润_TTM']
        result_df['PE_TTM'] = result_df['PE_TTM'].apply(func=lambda x: None if x <= 0 else x)
        result_df['PE_TTM_0'] = result_df['总市值_0'] / result_df['净利润_TTM_0']
        result_df = result_df.loc['2011':'2022']
    elif code_name in stock_dict.keys():
        result_df = year_data[year_data['code'] == stock_dict[code_name]].drop(columns=['code'])
        result_df.fillna(0, inplace=True)
        result_df['PE_TTM'] = result_df['总市值'] / result_df['净利润_TTM']
        result_df['PE_TTM'] = result_df['PE_TTM'].apply(func=lambda x: None if x <= 0 else x)
        result_df['PE_TTM_0'] = result_df['PE_TTM'].shift(1)
        result_df = result_df[result_df['年份'].between(2011, 2022)].set_index('年份')
    result_df['实际收益'] = np.log((result_df['总市值'] + result_df['分红市值']) / result_df['总市值_0'])
    result_df['股息驱动'] = np.log(result_df['分红市值'] / result_df['总市值'] + 1)
    result_df['业绩驱动'] = np.log(result_df['净利润_TTM'] / result_df['净利润_TTM_0'])
    result_df['估值驱动'] = np.log(result_df['PE_TTM'] / result_df['PE_TTM_0'])
    #
    # 业绩驱动修正项
    for year in result_df.index:
        profit_ = result_df['净利润_TTM'][year]
        profit_0 = result_df['净利润_TTM_0'][year]
        ret_actual = result_df['实际收益'][year]
        div_ = result_df['股息驱动'][year]
        if (profit_ < 0) & (profit_0 > 0):
            profit_ret_adj = (profit_ - profit_0) / profit_0
            result_df['业绩驱动'][year] = profit_ret_adj
            result_df['估值驱动'][year] = ret_actual - div_ - profit_ret_adj
        elif (profit_ > 0) & (profit_0 < 0):
            result_df['业绩驱动'][year] = (ret_actual - div_) / 2
            result_df['估值驱动'][year] = (ret_actual - div_) / 2
        elif (profit_ < 0) & (profit_0 < 0):
            profit_ret_adj = np.log(profit_0 / profit_)
            result_df["业绩驱动"][year] = profit_ret_adj
            result_df["估值驱动"][year] = ret_actual - div_ - profit_ret_adj
    #
    result_df['模型估计收益'] = result_df['股息驱动'] + result_df['业绩驱动'] + result_df['估值驱动']
    #
    path_save = os.path.join(folder_save, '拆分结果_' + code_name + '.csv')
    result_df.to_csv(path_save, encoding="gbk")
    return result_df


if __name__ == '__main__':
    # check_index('沪深300')
    stock_list = ['东方财富', '贵州茅台', '恒瑞医药']
    broad_index_list = ['沪深300', '中证500', '上证50', '创业板指', '红利指数']
    ind_index_list = ['石油石化', '煤炭', '有色金属', '电力及公用事业', '钢铁', '基础化工', '建筑', '建材', '轻工制造', '机械', '电力设备及新能源', '国防军工', '汽车', '商贸零售', '消费者服务', '家电', '纺织服装', '医药', '食品饮料', '农林牧渔', '银行', '非银行金融', '房地产', '交通运输', '电子', '通信', '计算机', '传媒', '综合', '综合金融']
    all = stock_list + broad_index_list + ind_index_list
    for name in broad_index_list:
        print('\n', name)
        split_process(name)
    # code = '300059.SZ'
    # result = check_single_stock(code)
