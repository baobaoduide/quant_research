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
    #
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
    code_indicator = pd.concat([code_indicator, data_lack1])
    code_indicator.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    code_indicator.reset_index(drop=True, inplace=True)
    # code_indicator和code_daily_detail_cut是重要数据。下面基于这两个数据进一步筛选样本
    # 筛选信息：板块市场要求
    # code_board_list = code_all[code_all['code'].apply(func=lambda x: x[-2:] in ['SH', 'SZ'])]
    code_board_list = basic_info[basic_info['list_board'].isin(['科创板'])]
    temp = code_indicator[code_indicator['code'].isin(code_board_list['code'])].sort_values(by=['mv_mean'], ascending=False).reset_index(drop=True)
    # 筛选信息：上市时间要求
    code_list = code_all[code_all['list_board'].isin(['科创板'])]
    cut_month = -12
    date_cut = cal_change_month(end_date, cut_month)
    code_list_over = code_list[code_list['date_in'] < date_cut]
    code_list_within = code_list[code_list['date_in'] >= date_cut]
    within_check_data = code_indicator[code_indicator['code'].isin(code_list['code'])]
    within_check_data.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    top5_code = within_check_data.iloc[:5]
    top5_code = top5_code[top5_code['code'].isin(code_list_within['code'])]
    top5_code = pd.merge(top5_code, basic_info[['code', 'date_in']], on=['code'], how='left')
    top5_code['date_cut'] = top5_code['date_in'].apply(func=lambda x: cal_change_month(x, 3))
    date_cut = '2022-05-18'
    top5_code = top5_code[top5_code['date_cut'] < date_cut]
    code_all = pd.concat([code_list_over['code'], top5_code['code']])
    # 确立样本空间
    sample_space = code_indicator[code_indicator['code'].isin(code_all)]
    sample_space = sample_space[sample_space['code'].isin(code_board_list['code'])]
    mv_lack = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\九号公司总市值.xlsx').rename(
        columns={'日期': 'date', '九号公司-WD': 'mv_mean'})
    amount_lack = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\九号公司成交额.xlsx').rename(
        columns={'日期': 'date', '九号公司-WD': 'amount_mean'})
    data_lack = pd.merge(mv_lack, amount_lack, on='date')
    data_lack1 = pd.DataFrame(
        {'code': ['689009.SH'], 'name': ['九号公司-WD'], 'mv_mean': [data_lack['mv_mean'].mean() / 10000],
         'amount_mean': [data_lack['amount_mean'].mean() / 1000], 'num_days': len(data_lack), 'profit': 410598753.65})
    sample_space = pd.concat([sample_space, data_lack1])
    # 上面已经完成样本空间筛选
    # 成交金额筛选过程
    sample_space_use = sample_space.sort_values(by=['amount_mean'], ascending=False)
    ind_cut_amount = math.ceil(len(sample_space_use)*0.9)
    sample_space_use = sample_space_use.iloc[: ind_cut_amount]
    # 市值筛选过程
    sample_space_use.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    sample_space_use.reset_index(drop=True, inplace=True)
    # 缓冲区规则
    ind_larger = 60
    top_larger = sample_space_use.iloc[:ind_larger]
    old_in_top_larger = top_larger[top_larger['code'].isin(member_last)].reset_index(drop=True)
    ind_smaller = 40
    top_smaller = sample_space_use.iloc[:ind_smaller]
    new_in_top_smaller = top_smaller[~top_smaller['code'].isin(member_last)].reset_index(drop=True)
    print('上期样本数：', len(member_last))
    print('老样本优先范围内老样本数：', len(old_in_top_larger), '新样本优先范围内新样本数：', len(new_in_top_smaller))
    # 优先老样本44个，所以保留前45个老样本，加上4个新样本
    old_retain = old_in_top_larger
    new_add = sample_space_use[~sample_space_use['code'].isin(member_last)]
    member_in = new_add.iloc[:5]
    member_in.sort_values(by='code', inplace=True)
    member_in.reset_index(drop=True, inplace=True)
    result = pd.concat([old_retain, member_in]).sort_values(by=['code'])
    code_out = member_last[~member_last.isin(result['code'])]
    member_out = code_indicator_temp[code_indicator_temp['code'].isin(code_out)].sort_values(by='code').reset_index(drop=True)
    #
    old_detail = code_indicator_temp[code_indicator_temp['code'].isin(member_last)].sort_values(by=['mv_mean'])
    member_change = pd.concat([member_in, member_out], axis=1)
    print(len(result), len(member_in), len(member_out))
    path_data = os.path.join(path_save, '科创50_变动.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    path_data = os.path.join(path_save, '科创50_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


def predict_kc50(term, index_name):
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
    predict_kc50('2022-06-30', '科创50')
