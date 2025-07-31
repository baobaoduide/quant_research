import os
import pandas as pd
path_data_local = r'H:\database_local\stock'


def update_balancesheet():
    path_data = os.path.join(path_data_local, 'asharebalancesheet_to20220121.csv')
    rtn_1 = pd.read_csv(path_data)
    path_data = os.path.join(path_data_local, 'AShareBalanceSheet.csv')
    rtn_2 = pd.read_csv(path_data)
    rtn = pd.concat([rtn_1, rtn_2])
    rtn.drop_duplicates(subset=['S_INFO_WINDCODE', 'REPORT_PERIOD', 'STATEMENT_TYPE', 'ACTUAL_ANN_DT'], keep='last', inplace=True)
    path_new = os.path.join(path_data_local, 'asharebalancesheet_to20220510.csv')
    rtn.to_csv(path_new, encoding='gbk', index=False)
    return rtn


def update_ashare_income():
    path_data = os.path.join(path_data_local, 'ashareincome_to20220121.csv')
    rtn_1 = pd.read_csv(path_data)
    path_data = os.path.join(path_data_local, 'AShareIncome.csv')
    rtn_2 = pd.read_csv(path_data)
    rtn = pd.concat([rtn_1, rtn_2])
    rtn.drop_duplicates(subset=['S_INFO_WINDCODE', 'REPORT_PERIOD', 'STATEMENT_TYPE', 'ACTUAL_ANN_DT'], keep='last',
                        inplace=True)
    path_new = os.path.join(path_data_local, 'ashareincome_to20220510.csv')
    rtn.to_csv(path_new, encoding='gbk', index=False)
    return rtn


def update_ashareeodprices():
    path_data = os.path.join(path_data_local, 'AShareEODPrices1.csv')
    rtn_1 = pd.read_csv(path_data)
    path_data = os.path.join(path_data_local, 'AShareEODPrices2.csv')
    rtn_2 = pd.read_csv(path_data)
    path_data = os.path.join(path_data_local, 'AShareEODPrices_close_amount.csv')
    rtn_3 = pd.read_csv(path_data)
    rtn = pd.concat([rtn_1, rtn_2, rtn_3])
    print(rtn_3['TRADE_DT'].min(rtn_2['TRADE_DT'].max()), rtn.duplicated().sum(), rtn.duplicated(subset=['S_INFO_WINDCODE', 'TRADE_DT']).sum())
    rtn.drop_duplicates(subset=['S_INFO_WINDCODE', 'REPORT_PERIOD', 'STATEMENT_TYPE', 'ACTUAL_ANN_DT'], keep='last',
                        inplace=True)
    path_new = os.path.join(path_data_local, 'ashareincome_to20220510.csv')
    rtn.to_csv(path_new, encoding='gbk', index=False)
    return rtn


def cal_mv_mean(mv_code):
    mv_code['mv_mean'] = mv_code['mv'].expanding().mean()
    return mv_code


def cut_mv():
    name_dict1 = {'S_INFO_WINDCODE': 'code', 'TRADE_DT': 'date', 'S_VAL_MV': 'mv'}
    path_data = os.path.join(path_data_local, 'Stock_MVBPUse.csv')
    rtn1 = pd.read_csv(path_data, usecols=name_dict1.keys(), dtype={'TRADE_DT': str})
    rtn1.rename(columns=name_dict1, inplace=True)
    name_dict2 = {'S_INFO_WINDCODE': 'code', 'TRADE_DT': 'date', 'S_DQ_TRADESTATUS': 'mv'}
    path_data = os.path.join(path_data_local, 'marketValue_to20220510.csv')
    rtn2 = pd.read_csv(path_data, dtype={'TRADE_DT': str})
    rtn2.rename(columns=name_dict2, inplace=True)
    rtn = pd.concat([rtn1, rtn2])
    rtn.sort_values(by=['code', 'date'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    rtn['date'] = rtn['date'].apply(
        func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
    #
    mv_rank_temp = rtn.dropna(subset=['mv'])
    mv_rank_temp = mv_rank_temp.groupby(by=['code']).apply(cal_mv_mean)
    mv_rank_temp = mv_rank_temp[mv_rank_temp['code'].apply(func=lambda x: x[0] != '3')]
    mv_rank_temp['rank'] = mv_rank_temp.groupby(by=['date'])['mv_mean'].rank(ascending=False)
    mv_rank_temp.sort_values(by=['date', 'rank'], inplace=True)
    mv_rank_temp.reset_index(drop=True, inplace=True)
    path_data = r'H:\database_local\stock\temp_predict_indexmem\mv_rank_exgem_to20210427.csv'
    mv_rank_temp.to_csv(path_data, index=False)
    #
    temp = rtn[rtn['date'] >= '2015-01-01']
    path_temp = r'H:\database_local\stock\temp_predict_indexmem\market_data_20150101to20220509.csv'
    temp.to_csv(path_temp, index=False)
    pass


def cut_price_money():
    name_dict = {'S_INFO_WINDCODE': 'code', 'TRADE_DT': 'date', 'S_DQ_CLOSE': 'close', 'S_DQ_AMOUNT': 'amount', 'S_DQ_TRADESTATUS': 'is_trade'}
    path_data = os.path.join(path_data_local, 'AShareEODPrices1.csv')
    rtn1 = pd.read_csv(path_data, usecols=name_dict.keys(), dtype={'TRADE_DT': str})
    path_data = os.path.join(path_data_local, 'AShareEODPrices2.csv')
    rtn2 = pd.read_csv(path_data, usecols=name_dict.keys(), dtype={'TRADE_DT': str})
    path_data = os.path.join(path_data_local, 'AShareEODPrices_close_amount.csv')
    rtn3 = pd.read_csv(path_data, usecols=name_dict.keys(), dtype={'TRADE_DT': str})
    rtn = pd.concat([rtn1, rtn2, rtn3])
    rtn.rename(columns=name_dict, inplace=True)
    print(rtn.duplicated().sum())
    rtn.drop_duplicates(inplace=True)
    #
    rtn['date'] = rtn['date'].apply(
        func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
    rtn['is_trade'] = rtn['is_trade'].apply(func=lambda x: 0 if x == '停牌' else 1)
    temp = rtn[rtn['date'] >= '2015-01-01']
    path_temp = r'H:\database_local\stocktemp_predict_indexmem\close_money_20150101to20220509.csv'
    temp.to_csv(path_temp, index=False)
    pass


def prepare_profit_data():
    name_dict = {'S_INFO_WINDCODE': 'code', 'ACTUAL_ANN_DT': 'date', 'REPORT_PERIOD': 'report_period',
                 'STATEMENT_TYPE': 'statement_type', 'NET_PROFIT_EXCL_MIN_INT_INC': 'profit'}
    path_data = r'H:\database_local\stock\ashareincome_to20220510.csv'
    rtn = pd.read_csv(path_data, encoding='gbk', usecols=name_dict.keys(), dtype={'ACTUAL_ANN_DT': str, 'REPORT_PERIOD': str})
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn = rtn[rtn['statement_type'].isin([408001000, 408005000, 408027000, 408028000])]
    rtn.dropna(subset=['date'], inplace=True)
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn = rtn[rtn['report_period'] == '20211231']
    rtn.sort_values(by=['code', 'date'], inplace=True)
    rtn = rtn.groupby(by=['code'], as_index=False).last()
    rtn.reset_index(drop=True, inplace=True)
    rtn.to_csv(r'H:\database_local\stock\temp_predict_indexmem\profit_last_year.csv', index=False, encoding='gbk')
    return rtn


def prepare_profit_2y():
    name_dict = {'S_INFO_WINDCODE': 'code', 'ACTUAL_ANN_DT': 'date', 'REPORT_PERIOD': 'report_period',
                 'STATEMENT_TYPE': 'statement_type', 'NET_PROFIT_AFTER_DED_NR_LP': 'profit_kf', 'OPER_PROFIT': 'profit_oper', 'OPER_REV': 'rev_oper'}
    path_data = r'H:\database_local\stock\ashareincome_to20220510.csv'
    rtn = pd.read_csv(path_data, encoding='gbk', usecols=name_dict.keys(), dtype={'ACTUAL_ANN_DT': str, 'REPORT_PERIOD': str})
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn = rtn[rtn['statement_type'].isin([408001000, 408005000, 408027000, 408028000])]
    rtn.dropna(subset=['date'], inplace=True)
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn = rtn[rtn['report_period'].isin(['20211231', '20201231'])]
    rtn.sort_values(by=['code', 'date'], inplace=True)
    rtn = rtn.groupby(by=['code'], as_index=False).last()
    rtn.reset_index(drop=True, inplace=True)
    rtn.to_csv(r'H:\database_local\stock\temp_predict_indexmem\profit_rev_data_2y.csv', index=False, encoding='gbk')
    return rtn


if __name__ == '__main__':
    # update_ashare_income()
    # update_balancesheet()
    # update_ashareeodprices()
    # prepare_profit_data()
    # cut_price_money()
    # cut_mv()
    # cut_price_money()
    prepare_profit_2y()
