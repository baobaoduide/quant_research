import os
import pandas as pd
import math
from load_data_from_data_base import load_index_list, load_aindexmembers, load_industry_cate_zz
from util_common import check_index
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
    #
    key_words = ['建筑与工程', '建筑装修']
    industry_code = industry_zz[industry_zz['industry_lv3'].isin(key_words)]
    # 具体选样过程
    sample_space_use = sample_space.sort_values(by=['amount_mean'], ascending=False)
    ind_cut_amount = math.ceil(len(sample_space_use) * 0.8)
    sample_space_use = sample_space_use.iloc[:ind_cut_amount]
    #
    sample_space_use = sample_space_use[sample_space_use['code'].isin(industry_code['code'])]
    #
    sample_space_use.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    sample_space_use.reset_index(drop=True, inplace=True)
    top50 = sample_space_use.iloc[:50]
    #
    member_in = top50[~top50['code'].isin(member_last)].reset_index(drop=True)
    code_out = member_last[~member_last.isin(top50['code'])]
    member_out = code_indicator_temp[code_indicator_temp['code'].isin(code_out)].reset_index(drop=True)
    print('转入数：', len(member_in), '转出数：', len(member_out))
    member_change = pd.concat([member_in, member_out], axis=1)
    path_data = os.path.join(path_save, '基建工程_变动.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    result = top50
    path_data = os.path.join(path_save, '基建工程_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


if __name__ == '__main__':
    predict_index('2022-06-30', '基建工程')
