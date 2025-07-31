import pandas as pd


def load_eod_price_data():
    path_data = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\AShareEODPrices1.csv'
    rtn1 = pd.read_csv(path_data, usecols=['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_CLOSE', 'S_DQ_ADJFACTOR'], dtype={'TRADE_DT': str})
    path_data = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\AShareEODPrices2.csv'
    rtn2 = pd.read_csv(path_data, usecols=['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_CLOSE', 'S_DQ_ADJFACTOR'], dtype={'TRADE_DT': str})
    path_data = r'H:\TF_Intern\data_20220218\ashareeodprices.csv'
    rtn3 = pd.read_csv(path_data, usecols=['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_CLOSE', 'S_DQ_ADJFACTOR'], dtype={'TRADE_DT': str})
    rtn = pd.concat([rtn1, rtn2, rtn3], ignore_index=True)
    name_dict = {'S_INFO_WINDCODE': 'StockCode', 'TRADE_DT': 'date', 'S_DQ_CLOSE': 'close', 'S_DQ_ADJFACTOR': 'adj_factor'}
    rtn.rename(columns=name_dict, inplace=True)
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4]+'-'+x[4:6]+'-'+x[6:])
    rtn.drop_duplicates(inplace=True)
    rtn.sort_values(by=['StockCode', 'date'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def check_func(check_df):
    report_date = check_df['report_period']
    cal_date = int(check_df['date'][:4])
    if int(report_date[:4]) == cal_date:
        is_retain = True
    elif (int(report_date[:4]) == cal_date-1) & (report_date[4:] == '1231'):
        is_retain = True
    else:
        is_retain = False
    return is_retain


def adj_income_data(stock_income):
    code = stock_income['StockCode'].to_list()[0]
    report_year = stock_income['report_year'].to_list()[0]
    quarter_list = ['0331', '0630', '0930', '1231']
    quarter_dates = stock_income['report_period'].apply(func=lambda x: x[4:]).sort_values().to_list()
    dates = stock_income['date'].to_list()
    profits = stock_income['free_cash_flow'].to_list()
    #
    report_period = [report_year + quarter for quarter in quarter_list]
    if quarter_dates[-1] == '0331':
        profit = profits + [0]*3
        dates_new = dates*4
    elif quarter_dates[-1] == '0630':
        if len(quarter_dates) == 1:
            profit = [0]+[profits[0]]*3
            dates_new = dates * 4
        elif len(quarter_dates) == 2:
            pro_2 = profits[1]-profits[0]
            profit = [profits[0]] + [pro_2] + [0]*2
            dates_new = [dates[0]] + [dates[1]]*3
    elif quarter_dates[-1] == '0930':
        if len(quarter_dates) == 1:
            profit = [0]*2+profits+[0]
            dates_new = dates * 4
        elif len(quarter_dates) == 2:
            if quarter_dates[0] == '0331':
                pro_2 = profits[1]-profits[0]
                profit = [profits[0]] + [0] + [pro_2] + [0]
                dates_new = [dates[0]] + [dates[1]]*3
            elif quarter_dates[0] == '0630':
                pro_2 = profits[1] - profits[0]
                profit = [0] + [profits[0]] + [pro_2] + [0]
                dates_new = [dates[0]]*2 + [dates[1]]*2
        elif len(quarter_dates) == 3:
            profit = profits + [0]
            dates_new = dates + [dates[-1]]
    elif quarter_dates[-1] == '1231':
        if len(quarter_dates) == 1:
            profit = [0]*3 + [profits[0]]
            dates_new = dates * 4
        elif len(quarter_dates) == 2:
            if quarter_dates[0] == '0331':
                pro_2 = profits[1] - profits[0]
                profit = [profits[0]]+[0]*2+[pro_2]
                dates_new = [dates[0]]+[dates[1]]*3
            elif quarter_dates[0] == '0630':
                pro_2 = profits[1] - profits[0]
                profit = [0]+[profits[0]]+[0]+[pro_2]
                dates_new = [dates[0]]*2+[dates[1]]*2
            elif quarter_dates[0] == '0930':
                pro_1 = profits[1] - profits[0]
                profit = [0]*2+[profits[0]]+[pro_1]
                dates_new = [dates[0]]*3+[dates[1]]
        elif len(quarter_dates) == 3:
            if '0331' not in quarter_dates:
                profit = [0, profits[0], profits[1]-profits[0], profits[2]-profits[1]]
                dates_new = [dates[0]]*2+dates[1:]
            elif '0630' not in quarter_dates:
                profit = [profits[0], 0, profits[1]-profits[0], profits[2]-profits[1]]
                dates_new = [dates[0]]+[dates[1]]*2+[dates[-1]]
            elif '0930' not in quarter_dates:
                profit = [profits[0], profits[1]-profits[0], 0, profits[2]-profits[1]]
                dates_new = dates[:2]+[dates[-1]]*2
        elif len(quarter_dates) == 4:
            profit = [profits[0], profits[1]-profits[0], profits[2]-profits[1], profits[3]-profits[2]]
            dates_new = dates
    stock_income_adj = pd.DataFrame({'StockCode': [code]*4, 'date': dates_new, 'report_period': report_period, 'free_cash_flow': profit})
    return stock_income_adj


def load_asharecashflow():
    path_data = r'H:\TF_Intern\data_20220218\AshareCashFlow.csv'
    rtn = pd.read_csv(path_data, usecols=['S_INFO_WINDCODE', 'ACTUAL_ANN_DT', 'REPORT_PERIOD', 'FREE_CASH_FLOW'], dtype={'ACTUAL_ANN_DT': str, 'REPORT_PERIOD': str})
    name_dict = {'S_INFO_WINDCODE': 'StockCode', 'ACTUAL_ANN_DT': 'date', 'REPORT_PERIOD': 'report_period', 'FREE_CASH_FLOW': 'free_cash_flow'}
    rtn.rename(columns=name_dict, inplace=True)
    rtn.dropna(subset=['date', 'free_cash_flow'], inplace=True)
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4]+'-'+x[4:6]+'-'+x[6:])
    rtn.sort_values(by=['StockCode', 'report_period'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    #
    ind_retain = rtn.apply(check_func, axis=1)
    rtn = rtn[ind_retain]
    rtn.drop_duplicates(subset=['StockCode', 'report_period'], keep='first', inplace=True)
    rtn['report_year'] = rtn['report_period'].apply(func=lambda x: x[:4])
    rtn = rtn.groupby(by=['StockCode', 'report_year'], as_index=False).apply(adj_income_data).reset_index(drop=True)
    rtn['year'] = rtn['report_period'].apply(func=lambda x: x[:4]+'-12-31')
    rtn['year'] = rtn.groupby(by=['StockCode'])['year'].shift(-1)
    rtn.dropna(subset=['year'], inplace=True)
    rtn = rtn.groupby(by=['StockCode', 'year'], as_index=False)['free_cash_flow'].sum()
    path_data = r'H:\TF_Intern\data_20220218\FreeCashFlow_year.csv'
    rtn.to_csv(path_data, index=False)
    return rtn


def load_fcf():
    path_data = r'H:\TF_Intern\data_20220218\FreeCashFlow_year.csv'
    rtn = pd.read_csv(path_data)
    return rtn


def load_asharedescription():
    path_data = r'H:\TF_Intern\data_20220218\asharedescription.csv'
    rtn = pd.read_csv(path_data, usecols=['S_INFO_WINDCODE', 'S_INFO_COMPCODE'])
    name_dict = {'S_INFO_WINDCODE': 'StockCode', 'S_INFO_COMPCODE': 'comp_id'}
    rtn.rename(columns=name_dict, inplace=True)
    rtn.sort_values(by=['StockCode'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_AshareFinancialderivative():
    path_data = r'H:\TF_Intern\data_20220218\AshareFinancialderivative.csv'
    rtn = pd.read_csv(path_data, usecols=['S_INFO_COMPCODE', 'BEGINDATE', 'ENDDATE', 'INTERVAL_LENGTH', 'FISCALYEAR', 'FCFF', 'ROE', 'YOYEQUITY', 'YOYASSETS', 'YOYNETPROFIT', 'YOYNETPROFIT_DEDUCTED', 'YOY_OR', 'OPTOGR', 'ROIC', 'ASSETSTOEQUITY', 'NETPROFITMARGIN'], dtype={'ENDDATE': str})
    name_dict = {'S_INFO_COMPCODE': 'comp_id', 'BEGINDATE': 'date_s', 'ENDDATE': 'date', 'INTERVAL_LENGTH': 'interval_length', 'FISCALYEAR': 'fiscal_year'}
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn = rtn[~rtn['interval_length'].isin([3, 12])]
    rtn.sort_values(by=['comp_id', 'date'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    rtn = rtn.groupby(by=['comp_id', 'fiscal_year'], as_index=False).last()
    #
    rtn.drop(columns=['date_s', 'interval_length'], inplace=True)
    rtn['fiscal_year'] = rtn['fiscal_year'].astype(int)
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-12-31')
    rtn['ROE'] = rtn['ROE'] / 100
    rtn['YOYEQUITY'] = rtn['YOYEQUITY'] / 100
    rtn['YOYASSETS'] = rtn['YOYASSETS'] / 100
    rtn['YOYNETPROFIT'] = rtn['YOYNETPROFIT'] / 100
    rtn['YOYNETPROFIT_DEDUCTED'] = rtn['YOYNETPROFIT_DEDUCTED'] / 100
    rtn['YOY_OR'] = rtn['YOY_OR'] / 100
    rtn['OPTOGR'] = rtn['OPTOGR'] / 100
    rtn['ROIC'] = rtn['ROIC'] / 100
    rtn['NETPROFITMARGIN'] = rtn['NETPROFITMARGIN'] / 100
    return rtn


def load_AShareEODDerivativeIndicator():
    path_data = r'H:\TF_Intern\data_20220218\AShareEODDerivativeIndicator.csv'
    rtn = pd.read_csv(path_data,
                      usecols=['S_INFO_WINDCODE', 'TRADE_DT', 'S_VAL_MV', 'S_VAL_PB_NEW', 'S_VAL_PE_TTM', 'S_VAL_PCF_OCFTTM', 'S_VAL_PS_TTM'], dtype={'TRADE_DT': str})
    name_dict = {'S_INFO_WINDCODE': 'StockCode', 'TRADE_DT': 'date'}
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
    rtn['S_VAL_MV'] = rtn['S_VAL_MV'] * 10000
    rtn.sort_values(by=['StockCode', 'date'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_asharedividend():
    path_data = r'H:\TF_Intern\data_20220218\asharedividend.csv'
    rtn = pd.read_csv(path_data,
                      usecols=['S_INFO_WINDCODE', 'REPORT_PERIOD', 'EX_DT', 'CASH_DVD_PER_SH_PRE_TAX', 'CASH_DVD_PER_SH_AFTER_TAX', 'S_DIV_BASESHARE'], dtype={'REPORT_PERIOD': str, 'EX_DT': str})
    name_dict = {'S_INFO_WINDCODE': 'StockCode', 'EX_DT': 'date', 'REPORT_PERIOD': 'report_period',
                 'CASH_DVD_PER_SH_PRE_TAX': 'cash_div_ps_pre', 'CASH_DVD_PER_SH_AFTER_TAX': 'cash_div_ps_aft', 'S_DIV_BASESHARE': 'base_share'}
    rtn.rename(columns=name_dict, inplace=True)
    rtn.dropna(subset=['date'], inplace=True)
    #
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
    rtn.dropna(subset=['base_share'], inplace=True)
    rtn['base_share'] = rtn['base_share'] * 10000
    rtn.sort_values(by=['StockCode', 'report_period'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


# def load_aindexvaluation():
#     path_data = r'H:\TF_Intern\data_20220218\aindexvaluation.csv'
#     rtn = pd.read_csv(path_data,
#                       usecols=['S_INFO_WINDCODE', 'TRADE_DT', 'EST_YOYPROFIT_Y1',
#                                'EST_YOYPROFIT_Y2'], dtype={'TRADE_DT': str})
#     name_dict = {'S_INFO_WINDCODE': 'StockCode', 'TRADE_DT': 'date'}
#     rtn.rename(columns=name_dict, inplace=True)
#     rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
#     rtn.sort_values(by=['StockCode', 'date'], inplace=True)
#     rtn.reset_index(drop=True, inplace=True)
#     return rtn


if __name__ == '__main__':
    load_asharedividend()
    load_fcf()
    load_asharecashflow()
    # load_AshareFinancialderivative()
    # load_AShareEODDerivativeIndicator()
    load_eod_price_data()
    load_asharedescription()
