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
    #
    key_words = ['白酒']
    industry_code = industry_zz[industry_zz['industry_lv4'].isin(key_words)]
    # 具体选样过程
    sample_space_use = sample_space.sort_values(by=['amount_mean'], ascending=False)
    ind_cut_amount = math.ceil(len(sample_space_use) * 0.8)
    sample_space_use = sample_space_use.iloc[:ind_cut_amount]
    #
    sample_space_use = sample_space_use[sample_space_use['code'].isin(industry_code['code'])]
    #
    member_in = sample_space_use[~sample_space_use['code'].isin(member_last)]
    code_out = member_last[~member_last.isin(sample_space_use['code'])]
    member_out = sample_space_use[sample_space_use['code'].isin(code_out)]
    print('转入数：', len(member_in), '转出数：', len(member_out))
    member_change = member_in
    path_data = os.path.join(path_save, '中证白酒_变动.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    result = sample_space_use
    path_data = os.path.join(path_save, '中证白酒_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


if __name__ == '__main__':
    predict_index('2022-06-30', '中证白酒')
