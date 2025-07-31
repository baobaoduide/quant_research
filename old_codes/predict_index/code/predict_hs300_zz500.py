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
    path_save1 = r'H:\database_local\stock\temp_predict_indexmem\sample_space\sample_space_raw_indicator_all.csv'
    code_indicator_temp.to_csv(path_save1, encoding='gbk', index=False)
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
    code_board_list = code_all[code_all['code'].apply(func=lambda x: x[-2:] in ['SH', 'SZ'])]
    # code_board_list = basic_info[basic_info['list_board'].isin(['创业板'])]
    # 筛选信息：上市时间要求
    code_list_c = code_all[code_all['list_board'].isin(['科创板', '创业板'])]
    cut_month = -12
    date_cut_c = cal_change_month(end_date, cut_month)
    code_list_c = code_list_c[code_list_c['date_in'] < date_cut_c]
    code_list_c_not = code_all[~code_all['list_board'].isin(['科创板', '创业板'])]
    cut_month = -3
    date_cut_c_not = cal_change_month(end_date, cut_month)
    code_list_c_not_over = code_list_c_not[code_list_c_not['date_in'] < date_cut_c_not]
    code_list_c_not_within = code_list_c_not[code_list_c_not['date_in'] >= date_cut_c_not]
    within_check_data = code_indicator[code_indicator['code'].apply(func=lambda x: x[-2:] in ['SZ', 'SH'])]
    within_check_data.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    top_code = within_check_data.iloc[:30]
    code_list_c_not_within = code_list_c_not_within[code_list_c_not_within['code'].isin(top_code['code'])]
    code_all = pd.concat([code_list_c, code_list_c_not_over, code_list_c_not_within])
    # 确立样本空间
    sample_space = code_indicator[code_indicator['code'].isin(code_all['code'])]
    sample_space = sample_space[sample_space['code'].isin(code_board_list['code'])]
    path_data = r'H:\database_local\stock\temp_predict_indexmem\sample_space\sample_space_hs300.csv'
    sample_space.to_csv(path_data, encoding='gbk', index=False)
    # 上面已经完成样本空间筛选
    # 成交金额筛选过程
    sample_space_use = sample_space.sort_values(by=['amount_mean'], ascending=False)
    ind_old_prior = math.ceil(len(sample_space_use)*0.6)
    code_prior_old = sample_space_use.iloc[: ind_old_prior]
    code_prior_old = code_prior_old[code_prior_old['code'].isin(member_last)]
    ind_cut_amount = math.ceil(len(sample_space_use)*0.5)
    code_cut_amount = sample_space_use.iloc[: ind_cut_amount]
    code_cut_amount_list = pd.concat([code_prior_old, code_cut_amount])['code'].drop_duplicates().to_list()
    sample_space_use = sample_space_use[sample_space_use['code'].isin(code_cut_amount_list)]
    # 市值筛选过程
    sample_space_use.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    sample_space_use.reset_index(drop=True, inplace=True)
    # 缓冲区规则
    ind_larger = 360
    top_larger = sample_space_use.iloc[:ind_larger]
    old_in_top_larger = top_larger[top_larger['code'].isin(member_last)].reset_index(drop=True)
    ind_smaller = 240
    top_smaller = sample_space_use.iloc[:ind_smaller]
    new_in_top_smaller = top_smaller[~top_smaller['code'].isin(member_last)].reset_index(drop=True)
    print('上期样本数：', len(member_last))
    print('老样本优先范围内老样本数：', len(old_in_top_larger), '新样本优先范围内新样本数：', len(new_in_top_smaller))
    # 下面是每只指数的个性化操作，目的都是准备成分股变化的表格和新样本表格
    # 沪深300老样本优先数272，新样本优先数20，未突破10%变动界限，都保留。另外在新样本中挑选前8位
    new_add = sample_space_use[~sample_space_use['code'].isin(member_last)]
    new_add = new_add[~new_add['code'].isin(new_in_top_smaller['code'])]
    new_add = new_add[new_add['profit'] > 0].reset_index(drop=True)
    new_add = new_add.iloc[:8]
    #
    member_in = pd.concat([new_in_top_smaller, new_add])
    member_in.sort_values(by='code', inplace=True)
    member_in.reset_index(drop=True, inplace=True)
    code_out = member_last[~member_last.isin(old_in_top_larger['code'])]
    member_out = code_indicator_temp[code_indicator_temp['code'].isin(code_out)].sort_values(by='code').reset_index(drop=True)
    member_change = pd.concat([member_in, member_out], axis=1)
    result = pd.concat([old_in_top_larger, member_in]).sort_values(by=['code'])
    print(len(result), len(member_in), len(member_out))
    path_data = os.path.join(path_save, '沪深300_变动.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    path_data = os.path.join(path_save, '沪深300_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


def predict_hs300(term, index_name):
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


def predict_zz500(term, index_name):
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    profit_data = load_profit_data()
    # 准备沪深300样本空间
    path_data = r'H:\database_local\stock\temp_predict_indexmem\sample_space\sample_space_hs300.csv'
    sample_space = pd.read_csv(path_data, encoding='gbk')
    path_data = r'H:\database_local\stock\temp_predict_indexmem\sample_space\sample_space_raw_indicator_all.csv'
    code_indicator_temp = pd.read_csv(path_data, encoding='gbk')
    # 准备沪深300成分股
    path_data = os.path.join(path_save, '沪深300_指数样本.csv')
    member_hs300 = pd.read_csv(path_data, encoding='gbk')
    # 具体选样过程
    sample_space_use = sample_space.sort_values(by=['mv_mean'], ascending=False)
    drop_prior = sample_space_use.iloc[:300]
    drop_code = pd.concat([member_hs300['code'], drop_prior['code']]).drop_duplicates()
    sample_space_use = sample_space_use[~sample_space_use['code'].isin(drop_code)]
    # 成交金额筛选过程
    sample_space_use.sort_values(by=['amount_mean'], ascending=False, inplace=True)
    ind_old_prior = math.ceil(len(sample_space_use) * 0.9)
    code_prior_old = sample_space_use.iloc[: ind_old_prior]
    code_prior_old = code_prior_old[code_prior_old['code'].isin(member_last)]
    ind_cut_amount = math.ceil(len(sample_space_use) * 0.8)
    code_cut_amount = sample_space_use.iloc[: ind_cut_amount]
    code_cut_amount_list = pd.concat([code_prior_old, code_cut_amount])['code'].drop_duplicates().to_list()
    sample_space_use = sample_space_use[sample_space_use['code'].isin(code_cut_amount_list)]
    # 市值筛选过程
    sample_space_use.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    sample_space_use.reset_index(drop=True, inplace=True)
    # 缓冲区规则
    ind_larger = 600
    top_larger = sample_space_use.iloc[:ind_larger]
    old_in_top_larger = top_larger[top_larger['code'].isin(member_last)].reset_index(drop=True)
    ind_smaller = 400
    top_smaller = sample_space_use.iloc[:ind_smaller]
    new_in_top_smaller = top_smaller[~top_smaller['code'].isin(member_last)].reset_index(drop=True)
    print('上期样本数：', len(member_last))
    print('老样本优先范围内老样本数：', len(old_in_top_larger), '新样本优先范围内新样本数：', len(new_in_top_smaller))
    top500 = sample_space_use.iloc[:500]
    old_in_top500 = top500[top500['code'].isin(member_last)].reset_index(drop=True)
    print('前500中旧样本数：', len(old_in_top500))
    code_old = member_last[member_last.isin(sample_space_use['code'])]
    old_data = sample_space_use[sample_space_use['code'].isin(code_old)]
    new_data = sample_space_use[~sample_space_use['code'].isin(code_old)]
    old_use = old_data.iloc[:450]
    member_in = new_data.iloc[:50]
    member_in.sort_values(by=['code'], inplace=True)
    member_in.reset_index(drop=True, inplace=True)
    code_out = member_last[~member_last.isin(old_use['code'])]
    member_out = code_indicator_temp[code_indicator_temp['code'].isin(code_out)].sort_values(by='code').reset_index(
        drop=True)
    member_change = pd.concat([member_in, member_out], axis=1)
    result = pd.concat([old_use, member_in]).sort_values(by=['code'])
    print(len(result), len(member_in), len(member_out))
    path_data = os.path.join(path_save, '中证500_变动.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    path_data = os.path.join(path_save, '中证500_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


if __name__ == '__main__':
    # predict_hs300('2022-06-30', '沪深300')
    predict_zz500('2022-06-30', '中证500')
