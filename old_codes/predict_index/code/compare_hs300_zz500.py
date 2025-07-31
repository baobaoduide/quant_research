import os
import pandas as pd
path_save = r'E:\NJU_term6\TF_Intern\predict_index\预测结果'
path_compare = r'E:\NJU_term6\TF_Intern\predict_index'


def check_result():
    path_data = os.path.join(path_save, '沪深300_变动.csv')
    member_change_hs300 = pd.read_csv(path_data, encoding='gbk')
    member_in = member_change_hs300.iloc[:, :2]
    member_out = member_change_hs300.iloc[:, 6:8]
    path_comp = os.path.join(path_compare, '沪深300_对照.xlsx')
    member_compare_hs300 = pd.read_excel(path_comp)
    member_in_compa = member_compare_hs300.iloc[:, :2]
    member_in_compa['股票代码'] = member_in_compa['股票代码'].apply(func=lambda x: str(x).zfill(6))
    member_in_compa['股票代码'] = member_in_compa['股票代码'].apply(func=lambda x: x+'.SH' if x[0] == '6' else x+'.SZ')
    member_out_comp = member_compare_hs300.iloc[:, 5:7]
    member_out_comp['股票代码.1'] = member_out_comp['股票代码.1'].apply(func=lambda x: str(x).zfill(6))
    member_out_comp['股票代码.1'] = member_out_comp['股票代码.1'].apply(func=lambda x: x + '.SH' if x[0] == '6' else x + '.SZ')
    #
    mem_in_check_hs300_1 = member_in[~member_in['code'].isin(member_in_compa['股票代码'])]
    mem_in_check_hs300_2 = member_in_compa[~member_in_compa['股票代码'].isin(member_in['code'])]
    mem_out_check_hs300_1 = member_out[~member_out['code.1'].isin(member_out_comp['股票代码.1'])]
    mem_out_check_hs300_2 = member_out_comp[~member_out_comp['股票代码.1'].isin(member_out['code.1'])]
    #
    #
    path_data = os.path.join(path_save, '中证500_变动.csv')
    member_change_zz500 = pd.read_csv(path_data, encoding='gbk')
    member_in = member_change_zz500.iloc[:, :2]
    member_out = member_change_zz500.iloc[:, 6:8]
    path_comp = os.path.join(path_compare, '中证500_对照.xlsx')
    member_compare_zz500 = pd.read_excel(path_comp)
    member_in_compa = member_compare_zz500.iloc[:, :2]
    member_in_compa['股票代码'] = member_in_compa['股票代码'].apply(func=lambda x: str(x).zfill(6))
    member_in_compa['股票代码'] = member_in_compa['股票代码'].apply(func=lambda x: x + '.SH' if x[0] == '6' else x + '.SZ')
    member_out_comp = member_compare_zz500.iloc[:, 5:7]
    member_out_comp['股票代码.1'] = member_out_comp['股票代码.1'].apply(func=lambda x: str(x).zfill(6))
    member_out_comp['股票代码.1'] = member_out_comp['股票代码.1'].apply(func=lambda x: x + '.SH' if x[0] == '6' else x + '.SZ')
    #
    mem_in_check_hs300_1 = member_in[~member_in['code'].isin(member_in_compa['股票代码'])]
    mem_in_check_hs300_2 = member_in_compa[~member_in_compa['股票代码'].isin(member_in['code'])]
    mem_out_check_hs300_1 = member_out[~member_out['code.1'].isin(member_out_comp['股票代码.1'])]
    mem_out_check_hs300_2 = member_out_comp[~member_out_comp['股票代码.1'].isin(member_out['code.1'])]
    return


if __name__ == '__main__':
    check_result()
