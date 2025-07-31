import pandas as pd
import statsmodels.api as sm
from old_codes.load_data.rawdata_util import load_ashare_financial_indicator, load_all_stock_basic_info


def get_rd_expenditure_data(expenditure_data, ashare_description):
    expenditure_data = pd.merge(ashare_description, expenditure_data, on=['Code_company'])
    expenditure_data.drop(columns=['Code_company'], inplace=True)
    expenditure_data.sort_values(by=['Code', 'ReportPeriod', 'CalcDate'], inplace=True)
    expenditure_data.reset_index(drop=True, inplace=True)
    return expenditure_data


def test_stock_industry_distribution(all_stock_industry):
    all_stock_industry['year'] = all_stock_industry['ipo_date'].apply(func=lambda x: x[:4])
    industry_year_count = all_stock_industry.groupby(by=['year', 'industry'])['Code'].count().rename('comp_num_listed')
    industry_year_count = industry_year_count.unstack(level=['industry']).fillna(0)
    industry_year_count = industry_year_count.astype(int).cumsum(axis=0)
    key_industrys = ['医药', '机械', '电子', '汽车', '通信', '计算机', '基础化工', '电力设备及新能源', '国防军工', '家电', '轻工制造']
    key_industry_year_count = industry_year_count[key_industrys]
    return industry_year_count


def test_rd_cover_ratio(expense_data, asharedescription):
    expense_data['report_year'] = expense_data['ReportPeriod'].apply(func=lambda x: int(x[:4]))
    expense_data['report_quarter'] = expense_data['ReportPeriod'].apply(func=lambda x: int(int(x[4:6]) / 3))
    #
    rd_expense_dropna = expense_data.dropna(subset=['rd_expense'])
    rd_expense_report_num = rd_expense_dropna.groupby(by=['report_year', 'report_quarter'])['Code'].count()
    rd_expense_report_num = rd_expense_report_num.unstack(level='report_quarter')
    rd_expense_dropna.sort_values(by=['Code', 'CalcDate'], inplace=True)
    rd_expense_dropna['ReportPeriod'] = rd_expense_dropna['ReportPeriod'].astype(int)
    report_period_cummax = rd_expense_dropna.groupby(by=['Code'])['ReportPeriod'].cummax()
    temp = rd_expense_dropna[rd_expense_dropna['ReportPeriod'] != report_period_cummax]
    #
    expense_data_use = expense_data[expense_data['report_quarter'] == 4]
    expense_data_use.drop(columns=['report_quarter'], inplace=True)
    expense_data_industry = pd.merge(expense_data_use, asharedescription[['Code', 'industry']], on=['Code'])
    #
    cal_cover_ratio_df = expense_data_industry.groupby(by=['report_year', 'industry', 'Code'])['rd_expense'].agg(is_rd=lambda x: int(~x.isna().all()))
    cal_cover_ratio_df.reset_index('Code', drop=True, inplace=True)
    cal_cover_ratio_df = cal_cover_ratio_df.groupby(level=cal_cover_ratio_df.index.names)['is_rd'].agg(rd_ratio=lambda x: x.sum() / len(x))
    cal_cover_ratio_df = cal_cover_ratio_df.unstack(level='industry')
    #
    idx = pd.IndexSlice
    key_industry_cover_ratio = cal_cover_ratio_df.loc[:, idx[:, ['医药', '机械', '电子', '汽车', '通信', '计算机', '基础化工', '电力设备及新能源', '国防军工', '家电', '轻工制造']]]
    key_industry_cover_ratio = key_industry_cover_ratio[(key_industry_cover_ratio.index >= 2005) & (key_industry_cover_ratio.index <= 2017)]
    return cal_cover_ratio_df, rd_expense_report_num


def reg_data(expense_data_industry_sub):
    if len(expense_data_industry_sub) >= 2:
        X = expense_data_industry_sub['rd_expense']
        y = expense_data_industry_sub['profit']
        X = sm.add_constant(X)
        results = sm.OLS(y, X).fit()
        beta = results.params[1]
        t_value = results.tvalues[1]
        rsquare_adj = results.rsquared_adj
    else:
        beta = None
        t_value = None
        rsquare_adj = None
    return beta, t_value, rsquare_adj


def test_rdexpense_profit(fin_indicator_rd_expense, ashare_industry):
    fin_indicator_rd_expense['report_year'] = fin_indicator_rd_expense['ReportPeriod'].apply(func=lambda x: int(x[:4]))
    fin_indicator_rd_expense['report_quarter'] = fin_indicator_rd_expense['ReportPeriod'].apply(func=lambda x: int(int(x[4:6]) / 3))
    expense_data_use = fin_indicator_rd_expense[fin_indicator_rd_expense['report_quarter'] == 4]
    expense_data_use.drop(columns=['report_quarter', 'ReportPeriod', 'CalcDate'], inplace=True)
    #
    expense_data_unstack = expense_data_use.set_index(keys=['report_year', 'Code'])
    expense_data_unstack = expense_data_unstack.unstack(level=['Code']).fillna(0)
    window_size = 3
    expense_data_unstack['rd_expense'] = expense_data_unstack['rd_expense'].rolling(window=window_size).sum()
    expense_data_unstack['profit'] = expense_data_unstack['profit'].shift(-2) - expense_data_unstack[
        'profit']
    expense_profit_data = expense_data_unstack.stack(level='Code').reset_index().dropna(subset=['rd_expense', 'profit'])
    expense_profit_data = expense_profit_data[expense_profit_data['rd_expense'] != 0]
    expense_data_industry = pd.merge(expense_profit_data, ashare_industry[['Code', 'industry']], on=['Code'])
    #
    rd_profit_relation = expense_data_industry.groupby(by=['report_year', 'industry']).apply(func=lambda x: x['rd_expense'].corr(x['profit'], method='spearman'))
    rd_profit_relation = rd_profit_relation.unstack('industry')
    #
    rd_profit_reg_coe = expense_data_industry.groupby(by=['report_year', 'industry']).apply(lambda x: reg_data(x)[0])
    rd_profit_reg_coe = rd_profit_reg_coe.unstack('industry')
    rd_profit_reg_coe_tvalue = expense_data_industry.groupby(by=['report_year', 'industry']).apply(lambda x: reg_data(x)[1])
    rd_profit_reg_coe_tvalue = rd_profit_reg_coe_tvalue.unstack('industry')
    rd_profit_reg_coe_rsquare = expense_data_industry.groupby(by=['report_year', 'industry']).apply(lambda x: reg_data(x)[2])
    rd_profit_reg_coe_rsquare = rd_profit_reg_coe_rsquare.unstack('industry')
    return rd_profit_relation, rd_profit_reg_coe


def check_rd_expense(start_date, end_date):
    # fields_expenditure = ['ann_item', 'item_name', 'item_amount']
    # rd_expenditure = load_wind_rd_expenditure(start_date, end_date, fields_expenditure)
    # ashare_description = load_asharedescription()
    # rd_expenditure = get_rd_expenditure_data(rd_expenditure, ashare_description)
    #
    fields_expense_finind = ['rd_expense', 'profit']
    fin_indicator_rd_expense = load_ashare_financial_indicator(start_date, end_date, fields_expense_finind)
    #
    fields_description = ['industry', 'ipo_date']
    all_stock_industry = load_all_stock_basic_info(fields_description)
    industry_year_count = test_stock_industry_distribution(all_stock_industry)
    ashare_industry, rd_expense_report_num = test_rd_cover_ratio(fin_indicator_rd_expense, all_stock_industry)
    #
    rd_profit_relation, rd_profit_reg_coe = test_rdexpense_profit(fin_indicator_rd_expense, all_stock_industry)
    return rd_profit_relation, rd_profit_reg_coe, ashare_industry, industry_year_count, rd_expense_report_num


start_date = '2000-01-01'
end_date = '2021-05-01'
check_df = check_rd_expense(start_date, end_date)
rd_profit_relation = check_df[0]
path_check_df = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\corr_with_profit_increase.csv'
rd_profit_relation.to_csv(path_check_df, encoding="utf_8_sig")
rd_profit_reg_coe = check_df[1]
path_check_df = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\reg_bata_with_profit_increase.csv'
rd_profit_reg_coe.to_csv(path_check_df, encoding="utf_8_sig")
rd_cover_ratio = check_df[2]
path_check_df = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\check_cover_ratio_temp.csv'
rd_cover_ratio.to_csv(path_check_df, encoding="utf_8_sig")
industry_year_count = check_df[3]
path_check_df = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\stock_listed_num_industry.csv'
industry_year_count.to_csv(path_check_df, encoding="utf_8_sig")
rd_expense_report_num = check_df[4]
path_check_df = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\rd_expense_report_num.csv'
rd_expense_report_num.to_csv(path_check_df, encoding="utf_8_sig")
