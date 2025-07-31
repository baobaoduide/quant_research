import os
import pandas as pd
from load_data_from_data_base import load_industry_cate_zz
path_save = r'E:\NJU_term6\TF_Intern\predict_index\预测结果'


def predict_index(term, index_name):
    member_last = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\证券公司成份(上期).xlsx')['code']
    print('上期样本数：', len(member_last))
    industry_zz = load_industry_cate_zz()
    # 准备中证全指样本空间
    path_data = r'H:\database_local\stock\temp_predict_indexmem\sample_space\sample_space_zz_industry.csv'
    sample_space = pd.read_csv(path_data, encoding='gbk')
    #
    key_words = ['证券公司']
    industry_code = industry_zz[industry_zz['industry_lv4'].isin(key_words)].reset_index(drop=True)
    # 具体选样过程
    sample_space_use = sample_space[sample_space['code'].isin(industry_code['code'])]
    member_in = sample_space_use[~sample_space_use['code'].isin(member_last)]
    member_change = member_in
    result = sample_space_use.sort_values(by=['code'])
    path_data = os.path.join(path_save, '证券公司_指数样本.csv')
    result.to_csv(path_data, encoding='gbk', index=False)
    return result, member_change


if __name__ == '__main__':
    predict_index('2022-06-30', '证券公司')
