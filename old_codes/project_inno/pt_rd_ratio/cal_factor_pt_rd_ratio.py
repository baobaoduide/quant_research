import os
import pandas as pd
from old_codes.load_data.load_factor import load_raw_factor_rd_expense, load_raw_factor_pt


def cal_pt_rd_ratio(rd_expense_factor, pt_factor):
    pt_factor['pt_num_all'] = pt_factor['TG3_B001'] + pt_factor['TG3_B002'] + pt_factor['TG3_B003'] + pt_factor['TG3_B004']
    pt_num_factor = pt_factor[['Code', 'CalcDate', 'pt_num_all']]
    rd_expense_factor['CalcDate'] = (pd.to_datetime(rd_expense_factor['CalcDate']) - pd.tseries.offsets.MonthEnd()).dt.strftime('%Y-%m-%d')
    pt_rd_ratio = pd.merge(rd_expense_factor, pt_num_factor, on=['Code', 'CalcDate'])
    pt_rd_ratio['pt_rd_ratio'] = pt_rd_ratio['pt_num_all']/pt_rd_ratio['rd_expense']*1000000
    pt_rd_ratio.drop(columns=['pt_num_all', 'rd_expense'], inplace=True)
    return pt_rd_ratio


def cal_factor(start_date, end_date):
    rd_expense_factor = load_raw_factor_rd_expense(start_date, end_date)
    pt_factor = load_raw_factor_pt(start_date, end_date)
    #
    factor = cal_pt_rd_ratio(rd_expense_factor, pt_factor)
    return factor


start_date = '2010-01-01'
end_date = '2020-12-31'
pt_rd_factor = cal_factor(start_date, end_date)
path_pt_rd_factor = os.path.join(os.getcwd(), 'pt_rd_factor.csv')
pt_rd_factor.to_csv(path_pt_rd_factor, index=False)
