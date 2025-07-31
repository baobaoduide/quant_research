import os
import math
import datetime
import pandas as pd
from load_data_from_data_base import load_asharedescription, load_asharest, load_close_money_temp, load_tradedays, load_asharetotal, load_ashareff, load_index_list, load_aindexmembers, load_industry_cate_zz, load_profit_data
from util_time import cal_change_month, get_change_trade_day, cal_tradeday_range, define_term
from util_common import prepare_mv_amount2, check_index
path_save = r'E:\NJU_term6\TF_Intern\predict_index\预测结果'


def prepare_space_space(basic_info, ashare_st, mv_amount, start_date, end_date,
                        tradedays, member_last, profit_data):
    # 准备完备的全部样本基本信息（添加ST剔除信息、上市第四天信息、计算日均金额市值开始日信息）
    today = datetime.date.today().strftime('%Y-%m-%d')
    code_all = basic_info.copy()
    code_all['date_out'].fillna(today, inplace=True)
    error_code = ashare_st[ashare_st['type'].isin(['S', 'Z', 'Y'])]
    error_code['date_out'].fillna(today, inplace=True)
    error_code['date_st'] = [cal_tradeday_range(s, e, tradedays, inclu='left').to_list() for s, e in
                             zip(error_code['date_in'], error_code['date_out'])]
    error_code_sum = error_code.groupby(by=['code'], as_index=False)['date_st'].sum()
    code_all = pd.merge(code_all, error_code_sum, on=['code'], how='left')
    delta_days = 3
    code_all['date_cut'] = code_all['date_in'].apply(
        func=lambda x: get_change_trade_day(x, delta_days, tradedays))
    code_all['date_start'] = code_all['date_cut'].apply(
        func=lambda x: x if x > start_date else start_date)
    # 合并全部行情数据
    code_daily_detail = pd.merge(code_all[['code', 'name', 'date_in', 'date_start', 'date_st']],
                                 mv_amount, on=['code'], how='left')
    # 筛选信息：保留可能样本（当前必须交易+样本期结束已上市）
    code_in_info = code_all[(code_all['date_out'] > end_date) & (code_all['date_in'] < end_date)]
    # 筛选信息：因为ST原因剔除的样本
    error_drop_dir = error_code[(error_code['date_out'] == today) & (error_code['date_in'] <= today)]
    # 剔除操作
    code_daily_detail_cut = code_daily_detail[code_daily_detail['date'] >= code_daily_detail['date_in']]
    code_daily_detail_cut = code_daily_detail_cut[
        code_daily_detail_cut['date_start'] <= code_daily_detail_cut['date']]  # 剔除新上市交易前3天
    code_daily_detail_cut = code_daily_detail_cut[code_daily_detail_cut['code'].isin(code_in_info['code'])]
    code_daily_detail_cut = code_daily_detail_cut[
        ~code_daily_detail_cut.apply(lambda x: x['date'] in x['date_st'] if isinstance(x['date_st'], list) else False,
                                     axis=1)]  # 剔除ST日期的子样本
    code_daily_detail_cut = code_daily_detail_cut[code_daily_detail_cut['is_trade'] == 1]  # 剔除停牌等没有交易日期
    code_daily_detail_cut = code_daily_detail_cut[~code_daily_detail_cut['code'].isin(error_drop_dir['code'])]
    # code_daily_detail.dropna(subset=['mv'], inplace=True)
    # 整理得出样本空间结果
    code_daily_detail_cut = code_daily_detail_cut[['code', 'name', 'date', 'mv', 'amount', 'mv_f_adj']]
    mv_half = code_daily_detail_cut.loc[code_daily_detail_cut['date'] >= '2021-11-01', ['code', 'name', 'mv']]
    mv_half = mv_half.groupby(by=['code', 'name'], as_index=False)['mv'].mean()
    mv_half.rename(columns={'mv': 'mv_mean'}, inplace=True)
    amount_year = code_daily_detail_cut[['code', 'name', 'amount']]
    amount_year = amount_year.groupby(by=['code', 'name'], as_index=False)['amount'].mean()
    amount_year.rename(columns={'amount': 'amount_mean'}, inplace=True)
    code_indicator = pd.merge(mv_half, amount_year, on=['code'])
    code_indicator.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    code_indicator.reset_index(drop=True, inplace=True)
    # code_indicator和code_daily_detail_cut是重要数据。下面基于这两个数据进一步筛选样本
    # 筛选信息：板块市场要求
    code_board_list = code_all[code_all['code'].apply(func=lambda x: x[-2:] in ['SH', 'SZ'])]
    # code_board_list = basic_info[basic_info['list_board'].isin(['创业板'])]
    # 筛选信息：上市时间要求
    code_list_c = code_all[code_all['list_board'].isin(['科创板'])]
    cut_month = -12
    date_cut_c = cal_change_month(end_date, cut_month)
    code_list_c = code_list_c[code_list_c['date_in'] < date_cut_c]
    code_list_c_not = code_all[~code_all['list_board'].isin(['科创板'])]
    cut_month = -6
    date_cut_c_not = cal_change_month(end_date, cut_month)
    code_list_c_not_over = code_list_c_not[code_list_c_not['date_in'] < date_cut_c_not]
    code_list_c_not_within = code_list_c_not[code_list_c_not['date_in'] >= date_cut_c_not]
    within_check_data = code_indicator[code_indicator['code'].apply(func=lambda x: x[-2:] in ['SZ', 'SH'])]
    within_check_data.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    top_code = within_check_data.iloc[:3]
    code_list_c_not_within = code_list_c_not_within[code_list_c_not_within['code'].isin(top_code['code'])]
    code_all = pd.concat([code_list_c, code_list_c_not_over, code_list_c_not_within])
    # 确立样本空间
    sample_space = code_indicator[code_indicator['code'].isin(code_all['code'])]
    sample_space = sample_space[sample_space['code'].isin(code_board_list['code'])]
    # 上面已经完成样本空间筛选
    # 成交金额筛选过程
    sample_space_use = sample_space.sort_values(by=['amount_mean'], ascending=False)
    ind_cut_amount = math.ceil(len(sample_space_use) * 0.8)
    sample_space_use = sample_space_use.iloc[:ind_cut_amount]
    # 筛选的第二步。。。
    path_data = r''  # 待修改
    industry_code = pd.read_csv(path_data)
    sample_space_use = sample_space_use[sample_space_use['code'].isin(industry_code['code'])]
    #
    sample_space_use.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    sample_space_use.reset_index(drop=True, inplace=True)
    top50 = sample_space_use.iloc[:30]
    #
    member_in = top50[~top50['code'].isin(member_last)].reset_index(drop=True)
    code_out = member_last[~member_last.isin(top50['code'])]
    member_out = code_indicator[code_indicator['code'].isin(code_out)].reset_index(drop=True)
    print('转入数：', len(member_in), '转出数：', len(member_out))
    member_change = pd.concat([member_in, member_out], axis=1)
    path_data = os.path.join(path_save, '国证芯片_指数样本.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    result = top50
    path_data = os.path.join(path_save, '国证芯片_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


def predict_index(term, index_name):
    path_data = r'H:\database_local\stock\temp_predict_indexmem\MSCI中国A50互联互通成份.xlsx'
    member_last = pd.read_excel(path_data)
    #
    path_data = r'H:\database_local\stock\temp_predict_indexmem\互联互通A股投资成份.xlsx'
    sample_space = pd.read_excel(path_data)
    return sample_space


if __name__ == '__main__':
    predict_index('2022-06-30', 'MSCI中国A50互联互通')
