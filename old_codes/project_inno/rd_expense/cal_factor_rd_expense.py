import numpy as np
import pandas as pd
from old_codes.load_data.rawdata_util import load_ashare_financial_indicator, load_data_from_wind_income


def get_year_report_date(income_data):
    year_report_date = income_data.copy()
    year_report_date['report_year'] = year_report_date['ReportPeriod'].apply(func=lambda x: int(x[:4]))
    year_report_date['report_quarter'] = year_report_date['ReportPeriod'].apply(func=lambda x: int(int(x[4:6])/3))
    year_report_date = year_report_date[year_report_date['report_quarter'] == 4]
    year_report_date.drop(columns=['report_quarter', 'ReportPeriod'], inplace=True)
    calc_year = year_report_date['CalcDate'].apply(func=lambda x: int(x[:4]))
    year_report_date = year_report_date[calc_year == year_report_date['report_year']+1]
    year_report_date = year_report_date.groupby(by=['Code', 'report_year'], as_index=False).first()
    return year_report_date


def cal_rd_factor(fin_indicator_rd_expense, year_report_date):
    rd_expense_data = fin_indicator_rd_expense.dropna(subset=['rd_expense'])
    rd_expense_data['report_year'] = rd_expense_data['ReportPeriod'].apply(func=lambda x: int(x[:4]))
    rd_expense_data['report_quarter'] = rd_expense_data['ReportPeriod'].apply(func=lambda x: int(int(x[4:6])/3))
    rd_expense_data = rd_expense_data[rd_expense_data['report_quarter'] == 4]
    rd_expense_data.drop(columns=['ReportPeriod', 'report_quarter'], inplace=True)
    #
    rd_expense_data_unstack = rd_expense_data.set_index(keys=['report_year', 'Code'])
    rd_expense_data_unstack = rd_expense_data_unstack.unstack(level=['Code'])
    report_year_start = rd_expense_data['report_year'].min()
    report_year_end = rd_expense_data['report_year'].max()
    report_years = pd.Index(list(range(report_year_start, report_year_end+1)))
    rd_expense_data_unstack = rd_expense_data_unstack.reindex(report_years)
    #
    rd_expense_data_unstack['rd_expense'] = rd_expense_data_unstack['rd_expense'].fillna(0)
    look_back_win = 3
    rd_expense_data_unstack['rd_expense'] = rd_expense_data_unstack['rd_expense'].rolling(window=look_back_win).apply(func=lambda x: x.dot(np.ones((look_back_win, 1))/look_back_win))
    factor_rd_expense = rd_expense_data_unstack.stack(level=['Code'])
    factor_rd_expense.reset_index(inplace=True)
    factor_rd_expense.rename(columns={'level_0': 'report_year'}, inplace=True)
    factor_rd_expense.sort_values(by=['Code', 'report_year'], inplace=True)
    factor_rd_expense = factor_rd_expense[factor_rd_expense['rd_expense'] != 0.0]
    replace_date = pd.merge(factor_rd_expense[['Code', 'report_year']], year_report_date, on=['Code', 'report_year'], how='left')
    factor_rd_expense.reset_index(drop=True, inplace=True)
    factor_rd_expense['CalcDate'] = factor_rd_expense['CalcDate'].mask(factor_rd_expense['CalcDate'].isna(), replace_date['CalcDate'])
    factor_rd_expense.drop(columns=['report_year'], inplace=True)
    factor_rd_expense.dropna(subset=['rd_expense', 'CalcDate'], inplace=True)
    factor_rd_expense.reset_index(drop=True, inplace=True)
    return factor_rd_expense


def cal_factor(start_date, end_date):
    fields_income = []
    read_type = 'csv'
    income_data = load_data_from_wind_income(start_date, end_date, fields_income, read_type)
    year_report_date = get_year_report_date(income_data)
    #
    fields_expense_finind = ['rd_expense']
    fin_indicator_rd_expense = load_ashare_financial_indicator(start_date, end_date, fields_expense_finind)
    factor_rd_expense = cal_rd_factor(fin_indicator_rd_expense, year_report_date)
    return factor_rd_expense
