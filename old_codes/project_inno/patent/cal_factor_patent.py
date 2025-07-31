import os
from project_innovation_ability.load_data.rawdata_util import load_patent_data_increase


def cal_factor_pt_num_inv(patent_data_inv):
    patent_data_inv['pt_num_inv'] = patent_data_inv['pt_examing_num'] + patent_data_inv['pt_utility_num'] + patent_data_inv['pt_design_num'] + patent_data_inv['pt_authorized_num']
    factor_pt_num_inv_df = patent_data_inv[['CalcDate', 'Code', 'pt_num_inv']]
    return factor_pt_num_inv_df


def cal_factor_pt_num_inc(patent_data_inc):
    patent_data_inc['pt_authorized_num_all'] = patent_data_inc['TG3_B002'] + patent_data_inc['TG3_B003'] + patent_data_inc['TG3_B004']
    patent_data_inc['pt_former_quote_all'] = patent_data_inc['TG3_B026'] + patent_data_inc['TG3_B028']
    patent_data_inc['pt_next_quote_all'] = patent_data_inc['TG3_V004'] + patent_data_inc['TG3_V005'] + patent_data_inc['TG3_V006'] + patent_data_inc['TG3_V007']
    patent_data_inc['pt_former_quote_ex_pt'] = patent_data_inc['TG3_B028']
    patent_data_inc['pt_next_quote_invent_authorized'] = patent_data_inc['TG3_V007']
    fields_factors = ['Code', 'CalcDate',
                      'TG3_B001', 'TG3_B009', 'TG3_B012', 'TG3_V001', 'TG3_B015', 'TG3_B018', 'TG3_B006', 'TG3_B021',
                     'TG3_B004', 'TG3_B011', 'TG3_B014', 'TG3_V003', 'TG3_B017', 'TG3_B020', 'TG3_B008', 'TG3_B024', 'TG3_B005',
                     'TG3_B002', 'TG3_B010', 'TG3_B013', 'TG3_V002', 'TG3_B016', 'TG3_B019', 'TG3_B007', 'TG3_B022',
                     'TG3_B003', 'TG3_B023',
                     'pt_authorized_num_all', 'pt_former_quote_all', 'pt_next_quote_all', 'pt_former_quote_ex_pt', 'pt_next_quote_invent_authorized']
    factor_pt_num_inc = patent_data_inc[fields_factors]
    return factor_pt_num_inc


def cal_factor_pt_comp_inc(factor_pt_num_inc_df):
    factor_pt_num_inc_df['pt_examing_comp'] = factor_pt_num_inc_df['TG3_B001'] + factor_pt_num_inc_df['TG3_B009'] + factor_pt_num_inc_df['TG3_B012'] + factor_pt_num_inc_df['TG3_V001'] + factor_pt_num_inc_df['TG3_B015'] + factor_pt_num_inc_df['TG3_B018'] + factor_pt_num_inc_df['TG3_B006'] + factor_pt_num_inc_df['TG3_B021']
    factor_pt_num_inc_df['pt_authorized_comp'] = factor_pt_num_inc_df['TG3_B004'] + factor_pt_num_inc_df['TG3_B011'] + \
                                              factor_pt_num_inc_df['TG3_B014'] + factor_pt_num_inc_df['TG3_V003'] + \
                                              factor_pt_num_inc_df['TG3_B017'] + factor_pt_num_inc_df['TG3_B020'] + \
                                              factor_pt_num_inc_df['TG3_B008'] + factor_pt_num_inc_df['TG3_B024'] + factor_pt_num_inc_df['TG3_B005']
    factor_pt_num_inc_df['pt_utility_comp'] = factor_pt_num_inc_df['TG3_B002'] + factor_pt_num_inc_df['TG3_B010'] + \
                                                 factor_pt_num_inc_df['TG3_B013'] + factor_pt_num_inc_df['TG3_V002'] + \
                                                 factor_pt_num_inc_df['TG3_B016'] + factor_pt_num_inc_df['TG3_B019'] + \
                                                 factor_pt_num_inc_df['TG3_B007'] + factor_pt_num_inc_df['TG3_B022']
    factor_pt_comp_inc_df = factor_pt_num_inc_df[['Code', 'CalcDate', 'pt_examing_comp', 'pt_authorized_comp', 'pt_utility_comp', 'pt_authorized_num_all', 'pt_former_quote_all']]
    return factor_pt_comp_inc_df


def cal_factor(start_date, end_date):
    # fields_pt_inv = ['pt_examing_num', 'pt_utility_num', 'pt_design_num', 'pt_authorized_num']
    # patent_data_inv = load_patent_data_inventory(start_date, end_date, fields_pt_inv)
    # factor_pt_num_inv_df = cal_factor_pt_num_inv(patent_data_inv)
    #
    # fields_description = ['industry']
    # stock_basic_info = load_all_stock_basic_info(fields_description)
    fields_pt_inc = ['TG3_B001', 'TG3_B009', 'TG3_B012', 'TG3_V001', 'TG3_B015', 'TG3_B018', 'TG3_B006', 'TG3_B021',
                     'TG3_B004', 'TG3_B011', 'TG3_B014', 'TG3_V003', 'TG3_B017', 'TG3_B020', 'TG3_B008', 'TG3_B024', 'TG3_B005',
                     'TG3_B002', 'TG3_B010', 'TG3_B013', 'TG3_V002', 'TG3_B016', 'TG3_B019', 'TG3_B007', 'TG3_B022',
                     'TG3_B003', 'TG3_B023',
                     'TG3_B026', 'TG3_B028', 'TG3_V004', 'TG3_V005', 'TG3_V006', 'TG3_V007']
    patent_data_inc = load_patent_data_increase(start_date, end_date, fields_pt_inc)
    factor_pt_num_inc_df = cal_factor_pt_num_inc(patent_data_inc)
    return factor_pt_num_inc_df


start_date = '2004-01-01'
end_date = '2021-02-28'
factor_pt_num_inc = cal_factor(start_date, end_date)
path_factor_pt_num_inv = os.path.join(os.getcwd(), 'factor_pt_num_inc.csv')
factor_pt_num_inc.to_csv(path_factor_pt_num_inv, index=False)
