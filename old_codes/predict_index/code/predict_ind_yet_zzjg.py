import os
import pandas as pd
import math
from load_data_from_data_base import load_index_list, load_aindexmembers, load_industry_cate_zz
from util_common import check_index
from filter_industry_sample2 import filter_zzjg
path_save = r'E:\NJU_term6\TF_Intern\predict_index\预测结果'


def predict_index(term, index_name):
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    industry_zz = load_industry_cate_zz()
    # 准备中证全指样本空间
    path_data = r'H:\database_local\stock\temp_predict_indexmem\sample_space\sample_space_zz_industry.csv'
    sample_space = pd.read_csv(path_data, encoding='gbk')
    path_data = r'H:\database_local\stock\temp_predict_indexmem\sample_space\sample_space_raw_indicator_all.csv'
    code_indicator_temp = pd.read_csv(path_data, encoding='gbk')
    # 具体选样过程
    sample_space_use = sample_space.sort_values(by=['amount_mean'], ascending=False)
    ind_cut_amount = math.ceil(len(sample_space_use) * 0.8)
    sample_space_use = sample_space_use.iloc[:ind_cut_amount]
    # 匹配信息
    path_bcinfo = r'H:\database_local\stock\temp_predict_indexmem\板块分类信息\公司补充信息.xlsx'
    bcinfo = pd.read_excel(path_bcinfo)
    bcinfo.rename(columns={'证券代码↑': 'code', '证券简称': 'name'}, inplace=True)
    sample_space_use = pd.merge(sample_space_use, bcinfo[['code', '主营产品名称', '主营产品类型', '公司简介']], on='code', how='left')
    # 筛选的第二步。。。
    filter_ind = filter_zzjg(term)
    sample_in_old = filter_ind[filter_ind.isin(member_last)]
    diff_code = member_last[~member_last.isin(filter_ind)]  # 将不在样本空间的股票先提出来
    change_num = len(sample_in_old)
    sample_space_use_ind = sample_space_use[sample_space_use['code'].isin(filter_ind)]
    #
    sample_space_use_ind.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    sample_space_use_ind.reset_index(drop=True, inplace=True)
    member_old = sample_space_use[sample_space_use['code'].isin(member_last)].sort_values(by='mv_mean', ascending=False)
    member_new = sample_space_use_ind[~sample_space_use_ind['code'].isin(member_last)].sort_values(by='mv_mean',
                                                                                                   ascending=False)
    path_data = os.path.join(path_save, '中证军工_入选空间.csv')
    member_new.to_csv(path_data, encoding='gbk', index=False)
    path_data = os.path.join(path_save, '中证军工_保留空间.csv')
    member_old.to_csv(path_data, encoding='gbk', index=False)
    top50 = sample_space_use.iloc[:change_num+80-64]
    #
    member_in = top50[~top50['code'].isin(member_last)].reset_index(drop=True)
    code_out = member_last[~member_last.isin(top50['code'])]
    member_out = code_indicator_temp[code_indicator_temp['code'].isin(code_out)]
    member_out = member_out[~member_out['code'].isin(diff_code)].reset_index(drop=True)
    print('转入数：', len(member_in), '转出数：', len(member_out))
    member_change = pd.concat([member_in, member_out], axis=1)
    path_data = os.path.join(path_save, '中证军工_变动.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    member_retain = code_indicator_temp[code_indicator_temp['code'].isin(diff_code)]
    result = pd.concat([top50, member_retain]).sort_values(by=['code']).reset_index(drop=True)
    path_data = os.path.join(path_save, '中证军工_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


if __name__ == '__main__':
    predict_index('2022-06-30', '中证军工') # 14只
