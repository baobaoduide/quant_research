import os
import pandas as pd


def cal_pt_factor_comp(pt_factor_std):
    pt_factor_std['pt_examing_comp'] = (pt_factor_std['TG3_B001'] + pt_factor_std['TG3_B009'] + pt_factor_std['TG3_B012'] + pt_factor_std['TG3_V001'] + pt_factor_std['TG3_B015'] + pt_factor_std['TG3_B018'] + pt_factor_std['TG3_B006'] + pt_factor_std['TG3_B021'])/8
    pt_factor_std['pt_authorized_comp'] = (pt_factor_std['TG3_B004'] + pt_factor_std['TG3_B011'] + \
                                                 pt_factor_std['TG3_B014'] + pt_factor_std['TG3_V003'] + \
                                                 pt_factor_std['TG3_B017'] + pt_factor_std['TG3_B020'] + \
                                                 pt_factor_std['TG3_B008'] + pt_factor_std['TG3_B024'] + \
                                                 pt_factor_std['TG3_B005'])/9
    pt_factor_std['pt_utility_comp'] = (pt_factor_std['TG3_B002'] + pt_factor_std['TG3_B010'] + \
                                              pt_factor_std['TG3_B013'] + pt_factor_std['TG3_V002'] + \
                                              pt_factor_std['TG3_B016'] + pt_factor_std['TG3_B019'] + \
                                              pt_factor_std['TG3_B007'] + pt_factor_std['TG3_B022'])/8
    pt_factor_complex = pt_factor_std[['Code', 'CalcDate', 'industry', 'pt_examing_comp', 'pt_authorized_comp', 'pt_utility_comp', 'pt_authorized_num_all', 'pt_former_quote_all']]
    #
    return pt_factor_complex


def cal_factor(start_date, end_date):
    path_pt_factor_std = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\factor_std_complete.csv'
    pt_factor_std = pd.read_csv(path_pt_factor_std)
    pt_factor_std = pt_factor_std[(pt_factor_std['CalcDate'] >= start_date) & (pt_factor_std['CalcDate'] <= end_date)]
    #
    pt_factor_complex = cal_pt_factor_comp(pt_factor_std)
    return pt_factor_complex


start_date = '2004-01-01'
end_date = '2021-02-28'
pt_factor_complex = cal_factor(start_date, end_date)
path_factor_pt_num_inv = os.path.join(os.getcwd(), 'pt_factor_complex.csv')
pt_factor_complex.to_csv(path_factor_pt_num_inv, index=False)
