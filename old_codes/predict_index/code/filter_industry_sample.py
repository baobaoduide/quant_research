import os
import pandas as pd
from load_data_from_data_base import load_index_list, load_aindexmembers, load_industry_cate_zz, load_main_business, load_cate_info_data
from util_common import check_index
path_folder = r'H:\database_local\stock\temp_predict_indexmem\sample_industry'


def filter_indu(term, index_name, key_words=[]):
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    industry_zz = load_industry_cate_zz()
    main_business = load_main_business()
    gn_board_info = load_cate_info_data()
    # 检查：概念板块信息
    key_word = '生物医药'
    board_gn_in = gn_board_info[gn_board_info['board_gn'].apply(func=lambda x: key_word in x if isinstance(x, str) else False)]
    board_gn_hot_in = gn_board_info[gn_board_info['board_gn_hot'].apply(func=lambda x: key_word in x if isinstance(x, str) else False)]
    board_industry_in = gn_board_info[gn_board_info['board_industry'].apply(func=lambda x: key_word in x if isinstance(x, str) else False)]
    comp_info = gn_board_info[
        gn_board_info['comp_brief'].apply(func=lambda x: key_word in x if isinstance(x, str) else False)]
    all_codes_boards = pd.concat([board_gn_in['code'], board_gn_hot_in['code'], board_industry_in['code']]).drop_duplicates()
    all_codes_in_old = all_codes_boards[all_codes_boards.isin(member_last)]
    print(len(board_gn_in), len(board_gn_hot_in), len(board_industry_in), len(comp_info), len(all_codes_boards), len(all_codes_in_old))
    # 下面的代码放弃
    # 检查：中证行业信息筛选
    industry_zz.sort_values(by=['industry_lv1', 'industry_lv2', 'industry_lv3', 'industry_lv4'], inplace=True)
    industry_lv1 = industry_zz['industry_lv1'].drop_duplicates()
    industry_lv2 = industry_zz['industry_lv2'].drop_duplicates()
    industry_lv3 = industry_zz['industry_lv3'].drop_duplicates()
    industry_lv4 = industry_zz['industry_lv4'].drop_duplicates()
    industry_csrc_m = industry_zz['industry_csrc_m'].drop_duplicates()
    industry_csrc_z = industry_zz['industry_csrc_z'].drop_duplicates() # 各级行业分类的类别细节
    check_industry_l1 = industry_zz[industry_zz['industry_lv1'].isin(key_words)]
    check_industry_l2 = industry_zz[industry_zz['industry_lv2'].isin(['医药'])]
    check_industry_l3 = industry_zz[industry_zz['industry_lv3'].isin(key_words)]
    check_industry_l4 = industry_zz[industry_zz['industry_lv4'].isin(key_words)]
    check_industry_csrc_m = industry_zz[industry_zz['industry_csrc_m'].isin(key_words)]
    check_industry_csrc_z = industry_zz[industry_zz['industry_csrc_z'].isin(key_words)]
    # 检查：主营业务信息筛选
    mainproduct_info = main_business[['code', 'name', 'main_product_type']].dropna(subset=['main_product_type'])
    mainproduct_info['main_product_type'] = mainproduct_info['main_product_type'].apply(
        func=lambda x: x.split('、'))
    mainproduct_info = mainproduct_info.explode('main_product_type')
    all_product_types = mainproduct_info['main_product_type'].drop_duplicates().sort_values()
    code_list_product_info = mainproduct_info.loc[
        mainproduct_info['main_product_type'].isin(key_words), ['code', 'name']].drop_duplicates()
    # 筛选：
    back_test = industry_zz[industry_zz['code'].isin(member_last)]
    code_list_zzind = industry_zz.loc[industry_zz['industry_lv1'].isin(['医药卫生']), ['code', 'name']]
    code_list_zzind_in_old = code_list_zzind[code_list_zzind['code'].isin(member_last)]
    print('中证行业指数中，新筛样本数：', len(code_list_zzind), '，属于旧样本的新样本数：', len(code_list_zzind_in_old))
    code_list_product_info_in_old = code_list_product_info[code_list_product_info['code'].isin(member_last)]
    print('万德主营业务信息中，新筛样本数：', len(code_list_product_info), '，属于旧样本的新样本数：', len(code_list_product_info_in_old))
    # 其他方式选样
    # 合并筛选样本
    code_list = pd.concat([code_list_zzind, code_list_product_info]).drop_duplicates().sort_values(by='code')
    code_list_in_old = code_list[code_list['code'].isin(member_last)]
    print('总，新筛总样本数：', len(code_list), '，属于旧样本的新总样本数：', len(code_list_in_old))
    #
    # path_save = os.path.join(path_folder, '选样_'+index_name+'.csv')
    # code_list.to_csv(path_save, index=False, encoding='gbk')
    return code_list


if __name__ == '__main__':
    # 需要进一步检验的指数有：
    index_list = ['结构调整', '光伏产业', '5G通信', 'CS新能车', '新能源车', '中证医疗', '中证军工', '中华半导体芯片', '国证芯片', '生物医药']
    key_word_list = ['', '光伏', '5G', '新能源汽车']
    for index in index_list[5:]:
        print(index)
        code_list = filter_indu('2022-06-30', index)
