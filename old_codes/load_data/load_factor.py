import os
import pandas as pd


def load_raw_factor_rd_expense(start_date, end_date):
    path_rd_factor = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\factor_rd_expense.csv'
    factor_df = pd.read_csv(path_rd_factor)
    factor_df = factor_df[(factor_df['CalcDate'] >= start_date) & (factor_df['CalcDate'] <= end_date)]
    return factor_df


def load_raw_factor_pt(start_date, end_date):
    path_pt_factor = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\factor_pt_num_inc.csv'
    factor_df = pd.read_csv(path_pt_factor)
    factor_df = factor_df[(factor_df['CalcDate'] >= start_date) & (factor_df['CalcDate'] <= end_date)]
    return factor_df


def load_raw_factor_pt_rd(start_date, end_date):
    path_rd_factor = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\pt_rd_factor.csv'
    factor_df = pd.read_csv(path_rd_factor)
    factor_df = factor_df[(factor_df['CalcDate'] >= start_date) & (factor_df['CalcDate'] <= end_date)]
    return factor_df


def load_std_factor_rd_expense(start_date, end_date, industry):
    path_rd_factor = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\rd_factor_std.csv'
    factor_df = pd.read_csv(path_rd_factor)
    factor_df = factor_df[(factor_df['CalcDate'] >= start_date) & (factor_df['CalcDate'] <= end_date)]
    factor_df = factor_df[factor_df['industry'] == industry]
    factor_df.drop(columns=['industry'], inplace=True)
    return factor_df


def load_std_factor_pt(start_date, end_date, factor_type, industry):
    path_route = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data'
    path_pt_factor = os.path.join(path_route, 'pt_factor_complex.csv')
    factor_df = pd.read_csv(path_pt_factor)
    factor_df = factor_df[(factor_df['CalcDate'] >= start_date) & (factor_df['CalcDate'] <= end_date)]
    columns = ['CalcDate', 'industry', 'Code']
    columns.append(factor_type)
    factor_df = factor_df[columns]
    factor_df = factor_df[factor_df['industry'] == industry]
    factor_df.reset_index(drop=True, inplace=True)
    factor_df.rename(columns={factor_type: 'factor_std'}, inplace=True)
    return factor_df


def load_std_factor_pt_rd(start_date, end_date, industry):
    path_route = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data'
    path_pt_factor = os.path.join(path_route, 'factor_std_pt_rd.csv')
    factor_df = pd.read_csv(path_pt_factor)
    factor_df = factor_df[(factor_df['CalcDate'] >= start_date) & (factor_df['CalcDate'] <= end_date)]
    factor_df = factor_df[factor_df['industry'] == industry]
    factor_df.reset_index(drop=True, inplace=True)
    return factor_df
