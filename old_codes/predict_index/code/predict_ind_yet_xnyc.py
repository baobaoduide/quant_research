import os
import pandas as pd
import math
from load_data_from_data_base import load_index_list, load_aindexmembers, load_industry_cate_zz
from util_common import check_index
from filter_industry_sample2 import filter_xnyc
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
    ind_cut_amount = math.ceil(len(sample_space_use) * 0.9)
    sample_space_use = sample_space_use.iloc[:ind_cut_amount]
    # 匹配信息
    path_bcinfo = r'H:\database_local\stock\temp_predict_indexmem\板块分类信息\公司补充信息.xlsx'
    bcinfo = pd.read_excel(path_bcinfo)
    bcinfo.rename(columns={'证券代码↑': 'code', '证券简称': 'name'}, inplace=True)
    sample_space_use = pd.merge(sample_space_use, bcinfo[['code', '主营产品名称', '主营产品类型', '公司简介']], on='code', how='left')
    # 筛选的第二步。。。
    filter_ind = filter_xnyc(term)
    sample_space_use_ind = sample_space_use[sample_space_use['code'].isin(filter_ind)]
    member_old = sample_space_use[sample_space_use['code'].isin(member_last)].sort_values(by='mv_mean', ascending=False)
    member_new = sample_space_use_ind[~sample_space_use_ind['code'].isin(member_last)].sort_values(by='mv_mean',
                                                                                                   ascending=False)
    path_data = os.path.join(path_save, '新能源车_入选空间.csv')
    member_new.to_csv(path_data, encoding='gbk', index=False)
    path_data = os.path.join(path_save, '新能源车_保留空间.csv')
    member_old.to_csv(path_data, encoding='gbk', index=False)
    #
    member_in = sample_space_use_ind[~sample_space_use_ind['code'].isin(member_last)].reset_index(drop=True)
    code_out = member_last[~member_last.isin(sample_space_use['code'])]
    member_out = code_indicator_temp[code_indicator_temp['code'].isin(code_out)].reset_index(drop=True)
    code_remain = member_last[member_last.isin(sample_space_use['code'])]
    member_remain = code_indicator_temp[code_indicator_temp['code'].isin(code_remain)].reset_index(drop=True)
    print('转入数：', len(member_in), '转出数：', len(member_out), '保留数：', len(member_remain))
    member_change = member_in
    result = pd.concat([member_remain, member_in]).reset_index(drop=True)
    #

    member_in_product = member_in
    path_data = os.path.join(path_save, '新能源车_变动.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    path_data = os.path.join(path_save, '新能源车_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


if __name__ == '__main__':
    predict_index('2022-06-30', '新能源车')
