import os
import pandas as pd
from load_data_from_data_base import load_index_list, load_aindexmembers, load_industry_cate_zz
from util_common import check_index
path_save = r'E:\NJU_term6\TF_Intern\predict_index\预测结果'


def predict_index(term, index_name):
    member_last = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\中证银行成份(上期).xlsx')['code']
    print('上期样本数：', len(member_last))
    industry_zz = load_industry_cate_zz()
    # 准备中证全指样本空间
    path_data = r'H:\database_local\stock\temp_predict_indexmem\sample_space\sample_space_zz_industry.csv'
    sample_space = pd.read_csv(path_data, encoding='gbk')
    #
    key_words = ['银行']
    industry_code = industry_zz[industry_zz['industry_lv2'].isin(key_words)]
    # 具体选样过程
    sample_space_use = sample_space[sample_space['code'].isin(industry_code['code'])] # 42
    member_in = sample_space_use[~sample_space_use['code'].isin(member_last)].reset_index(drop=True)
    code_out = member_last[~member_last.isin(sample_space_use['code'])]
    secu_out = sample_space[sample_space['code'].isin(code_out)]
    secu_out.sort_values(by=['mv_mean'], ascending=False, inplace=True)
    member_out = secu_out.iloc[7:].reset_index(drop=True)
    member_change = pd.concat([member_in, member_out], axis=1)
    path_data = os.path.join(path_save, '中证银行_变动.csv')
    member_change.to_csv(path_data, encoding='gbk', index=False)
    code_new = pd.concat([sample_space_use, secu_out.iloc[:7]]).drop_duplicates()
    result = sample_space[sample_space['code'].isin(code_new['code'])]
    path_data = os.path.join(path_save, '中证银行_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


if __name__ == '__main__':
    predict_index('2022-06-30', '中证银行')
