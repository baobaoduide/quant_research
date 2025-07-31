import os
import math
import datetime
import pandas as pd
from load_data_from_data_base import load_asharedescription, load_asharest, load_close_money_temp, load_tradedays, load_asharetotal, load_ashareff, load_index_list, load_aindexmembers, load_industry_cate_zz, load_profit_data
from util_time import cal_change_month, get_change_trade_day, cal_tradeday_range, define_term
from util_common import prepare_mv_amount2, check_index
from filter_industry_sample2 import filter_kccy50
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
    code_indicator_temp = code_daily_detail_cut.groupby(by=['code', 'name'], as_index=False).agg(
        mv_mean=pd.NamedAgg(column='mv', aggfunc='mean'), amount_mean=pd.NamedAgg(column='amount', aggfunc='mean'),
        num_days=pd.NamedAgg(column='date', aggfunc='count'))
    code_indicator_temp = pd.merge(code_indicator_temp, profit_data, on=['code'], how='left')
    mv_lack = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\九号公司总市值.xlsx').rename(
        columns={'日期': 'date', '九号公司-WD': 'mv_mean'})
    amount_lack = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\九号公司成交额.xlsx').rename(
        columns={'日期': 'date', '九号公司-WD': 'amount_mean'})
    data_lack = pd.merge(mv_lack, amount_lack, on='date')
    data_lack1 = pd.DataFrame(
        {'code': ['689009.SH'], 'name': ['九号公司-WD'], 'mv_mean': [data_lack['mv_mean'].mean() / 10000],
         'amount_mean': [data_lack['amount_mean'].mean() / 1000], 'num_days': len(data_lack), 'profit': 410598753.65})
    code_indicator_temp = pd.concat([code_indicator_temp, data_lack1])
    # path_save = r'H:\database_local\stock\temp_predict_indexmem\sample_space\sample_space_raw_indicator_all.csv'
    # code_indicator_temp.to_csv(path_save, encoding='gbk', index=False)
    code_daily_detail_cut = code_daily_detail_cut[code_daily_detail_cut['code'].isin(code_in_info['code'])]
    code_daily_detail_cut = code_daily_detail_cut[
        ~code_daily_detail_cut.apply(lambda x: x['date'] in x['date_st'] if isinstance(x['date_st'], list) else False,
                                     axis=1)]  # 剔除ST日期的子样本
    code_daily_detail_cut = code_daily_detail_cut[code_daily_detail_cut['is_trade'] == 1]  # 剔除停牌等没有交易日期
    code_daily_detail_cut = code_daily_detail_cut[~code_daily_detail_cut['code'].isin(error_drop_dir['code'])]
    # code_daily_detail.dropna(subset=['mv'], inplace=True)
    # 整理得出样本空间结果
    code_daily_detail_cut = code_daily_detail_cut[['code', 'name', 'date', 'mv', 'amount', 'mv_f_adj']]
    code_indicator = code_daily_detail_cut.groupby(by=['code', 'name'], as_index=False).agg(
        mv_mean=pd.NamedAgg(column='mv', aggfunc='mean'), amount_mean=pd.NamedAgg(column='amount', aggfunc='mean'), num_days=pd.NamedAgg(column='date', aggfunc='count'))
    code_indicator = pd.merge(code_indicator, profit_data, on=['code'], how='left')
    code_indicator.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    code_indicator.reset_index(drop=True, inplace=True)
    # code_indicator和code_daily_detail_cut是重要数据。下面基于这两个数据进一步筛选样本
    # 筛选信息：板块市场要求
    # code_board_list = code_all[code_all['code'].apply(func=lambda x: x[-2:] in ['SH', 'SZ'])]
    # code_board_list = basic_info[basic_info['list_board'].isin(['创业板'])]
    # 筛选信息：上市时间要求
    code_list_c = code_all[code_all['list_board'].isin(['科创板', '创业板'])]
    cut_month = -3
    date_cut_c = cal_change_month(end_date, cut_month)
    code_list_c_over = code_list_c[code_list_c['date_in'] < date_cut_c]
    code_list_c_within = code_list_c[code_list_c['date_in'] >= date_cut_c]
    within_check_data = code_indicator[code_indicator['code'].apply(func=lambda x: x[-2:] in ['SZ', 'SH'])]
    within_check_data.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    top_code = within_check_data.iloc[:30]
    code_list_c_not_within = code_list_c_within[code_list_c_within['code'].isin(top_code['code'])]
    code_all = pd.concat([code_list_c_over, code_list_c_not_within])
    # 确立样本空间
    sample_space = code_indicator[code_indicator['code'].isin(code_all['code'])]
    mv_lack = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\九号公司总市值.xlsx').rename(
        columns={'日期': 'date', '九号公司-WD': 'mv_mean'})
    amount_lack = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\九号公司成交额.xlsx').rename(
        columns={'日期': 'date', '九号公司-WD': 'amount_mean'})
    data_lack = pd.merge(mv_lack, amount_lack, on='date')
    data_lack1 = pd.DataFrame(
        {'code': ['689009.SH'], 'name': ['九号公司-WD'], 'mv_mean': [data_lack['mv_mean'].mean() / 10000],
         'amount_mean': [data_lack['amount_mean'].mean() / 1000], 'num_days': len(data_lack), 'profit': 410598753.65})
    # 上面已经完成样本空间筛选
    # 成交金额筛选过程
    sample_space_use = sample_space.sort_values(by=['amount_mean'], ascending=False)
    ind_cut_amount = math.ceil(len(sample_space_use) * 0.8)
    sample_space_use = sample_space_use.iloc[:ind_cut_amount]
    # 匹配信息
    path_bcinfo = r'H:\database_local\stock\temp_predict_indexmem\板块分类信息\公司补充信息.xlsx'
    bcinfo = pd.read_excel(path_bcinfo)
    bcinfo.rename(columns={'证券代码↑': 'code', '证券简称': 'name'}, inplace=True)
    sample_space_use = pd.merge(sample_space_use, bcinfo[['code', '主营产品名称', '主营产品类型', '公司简介']], on='code', how='left')
    # 筛选的第二步。。。
    filter_ind = filter_kccy50('2022-06-30')
    sample_space_use_ind = sample_space_use[sample_space_use['code'].isin(filter_ind)]
    sample_space_use_ind = pd.concat([sample_space_use_ind, data_lack1])
    #
    sample_space_use_ind.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    sample_space_use_ind.reset_index(drop=True, inplace=True)
    member_old = sample_space_use[sample_space_use['code'].isin(member_last)].sort_values(by='mv_mean', ascending=False)
    member_new = sample_space_use_ind[~sample_space_use_ind['code'].isin(member_last)].sort_values(by='mv_mean',
                                                                                                   ascending=False)
    path_data = os.path.join(path_save, '科创创业50_入选空间.csv')
    member_new.to_csv(path_data, encoding='gbk', index=False)
    path_data = os.path.join(path_save, '科创创业50_保留空间.csv')
    member_old.to_csv(path_data, encoding='gbk', index=False)
    #
    ind_larger = 60
    top_larger = sample_space_use_ind.iloc[:ind_larger]
    old_in_top_larger = top_larger[top_larger['code'].isin(member_last)].reset_index(drop=True)
    ind_smaller = 40
    top_smaller = sample_space_use_ind.iloc[:ind_smaller]
    new_in_top_smaller = top_smaller[~top_smaller['code'].isin(member_last)].reset_index(drop=True)
    print('老样本优先范围内老样本数：', len(old_in_top_larger), '新样本优先范围内新样本数：', len(new_in_top_smaller))
    old_data = sample_space_use_ind[sample_space_use_ind['code'].isin(member_last)].sort_values(by=['mv_mean'], ascending=False)
    old_data_in = old_data.iloc[:45]
    member_in = new_in_top_smaller.iloc[:5].reset_index(drop=True)
    #
    code_out = member_last[~member_last.isin(old_data_in['code'])]
    member_out = code_indicator_temp[code_indicator_temp['code'].isin(code_out)].reset_index(drop=True)
    print('转入数：', len(member_in), '转出数：', len(member_out))
    member_change = pd.concat([member_in, member_out], axis=1)
    path_data = os.path.join(path_save, '科创创业50_变动.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    result = pd.concat([old_data_in, member_in]).sort_values(by=['code'])
    path_data = os.path.join(path_save, '科创创业50_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


def predict_index(term, index_name):
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    #
    start_date, end_date = define_term(term)
    basic_info = load_asharedescription()
    ashare_st = load_asharest()
    share_total = load_asharetotal()
    share_ff = load_ashareff()
    money_data = load_close_money_temp(start_date, end_date)
    tradedays = load_tradedays().to_list()
    profit_data = load_profit_data()
    mv_amount = prepare_mv_amount2(share_total, share_ff, money_data, tradedays)
    #
    sample_space_detail = prepare_space_space(basic_info, ashare_st, mv_amount, start_date, end_date, tradedays, member_last, profit_data)
    return sample_space_detail


if __name__ == '__main__':
    predict_index('2022-06-30', '科创创业50')
