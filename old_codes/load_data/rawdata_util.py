import pandas as pd
import pymysql


def load_data_from_wind_income(start_date, end_date, flds, read_type):
    assert isinstance(flds, list)
    if read_type == "sql":
        name_dict = {'NetProfit': 'NET_PROFIT_EXCL_MIN_INT_INC', 'revenue': 'TOT_OPER_REV', 'admin_exp': 'LESS_GERL_ADMIN_EXP'}
        assert set(flds).issubset(name_dict.values())
        wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602',
                    'db': 'wind'}
        conn = pymysql.connect(charset='utf8', **wind_sql)
        if flds:
            cols_use = ",".join([name_dict[i] + " " + i for i in flds])
            rtn = pd.read_sql("select S_INFO_WINDCODE Code,ACTUAL_ANN_DT CalcDate,REPORT_PERIOD ReportPeriod,STATEMENT_TYPE StatementType,{0} "
                              "from ashareincome where ACTUAL_ANN_DT between {1} and {2}".format(
                                cols_use, start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
        else:
            rtn = pd.read_sql("select S_INFO_WINDCODE Code,ACTUAL_ANN_DT CalcDate,REPORT_PERIOD ReportPeriod,STATEMENT_TYPE StatementType "
                              "from ashareincome where ACTUAL_ANN_DT between {0} and {1}".format(
                               start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
    elif read_type == "csv":
        name_dict = {'S_INFO_WINDCODE': 'Code', 'ACTUAL_ANN_DT': 'CalcDate', 'REPORT_PERIOD': 'ReportPeriod',
                     'STATEMENT_TYPE': 'StatementType', 'NET_PROFIT_EXCL_MIN_INT_INC': 'NetProfit',
                     'TOT_OPER_REV': 'revenue', 'RD_EXPENSE': 'rd_expense', 'LESS_GERL_ADMIN_EXP': 'admin_exp'}
        # path_income = os.path.join(os.path.dirname(__file__), 'AShareIncome.csv')
        path_income = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\AShareIncome.csv'
        rtn = pd.read_csv(path_income, usecols=name_dict.keys(), dtype={'ACTUAL_ANN_DT': str, 'REPORT_PERIOD': str})
        rtn.rename(columns=name_dict, inplace=True)
        rtn = rtn[['Code', 'CalcDate', 'ReportPeriod', 'StatementType'] + flds]
        rtn.sort_values(by=['Code', 'ReportPeriod'], inplace=True)
        rtn = rtn[rtn['CalcDate'].between(start_date.replace("-", ""), end_date.replace("-", ""), inclusive='both')]
        rtn['StatementType'] = rtn['StatementType'].astype(str)
    else:
        assert False, "  error::gogoal_data>>load data>>read_type: {0} is unknown!".format(read_type)
    key_cols = ['Code', 'CalcDate', 'ReportPeriod']
    assert not rtn[key_cols].isna().any().any()
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    # rtn = rtn[rtn['StatementType'].isin(["408001000", "408004000", "408005000", "408027000", "408028000", "	408029000", "408050000"])]
    rtn = rtn[rtn['StatementType'].isin(
        ["408001000", "408005000", "408027000", "408028000"])]
    type_level_dict = {"408001000": 1, "408004000": 2, "408005000": 3, "408027000": 5, "408028000": 4,
                       "408029000": 7, "408050000": 6}
    rtn["优先级"] = rtn["StatementType"].apply(lambda x: type_level_dict[x])
    rtn.sort_values(["CalcDate", "Code", "ReportPeriod", "优先级"], ascending=True, inplace=True)
    rtn.drop_duplicates(["CalcDate", "Code", "ReportPeriod"], keep="first", inplace=True)
    rtn.drop(columns=['StatementType', '优先级'], inplace=True)
    rtn.sort_values(by=['Code', 'CalcDate', 'ReportPeriod'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_wind_rd_expense(start_date, end_date, flds):
    path_expense = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\data\asharerdexpense.csv'
    name_dict = {'S_INFO_COMPCODE': 'Code_company', 'REPORT_PERIOD': 'ReportPeriod', 'ANN_DT': 'CalcDate', 'ITEM_NAME': 'item_name', 'ITEM_AMOUNT': 'item_amount'}
    rtn = pd.read_csv(path_expense, usecols=name_dict.keys(), encoding='gbk', dtype={'REPORT_PERIOD': str, 'ANN_DT': str})
    rtn.rename(columns=name_dict, inplace=True)
    key_cols = ['Code_company', 'CalcDate', 'ReportPeriod']
    assert not rtn[key_cols].isna().any().any()
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn = rtn[key_cols + flds]
    rtn.sort_values(by=['Code_company', 'CalcDate', 'ReportPeriod'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_wind_rd_expenditure(start_date, end_date, flds):
    path_expenditure = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\data\rd_expenditure.csv'
    name_dict = {'S_INFO_COMPCODE': 'Code_company', 'REPORT_PERIOD': 'ReportPeriod', 'ANN_DT': 'CalcDate', 'STATEMENT_TYPE': 'StatementType', 'ANN_ITEM': 'ann_item', 'ITEM_NAME': 'item_name', 'ITEM_AMOUNT': 'item_amount'}
    rtn = pd.read_csv(path_expenditure, usecols=name_dict.keys(), dtype={'REPORT_PERIOD': str, 'ANN_DT': str})
    rtn.rename(columns=name_dict, inplace=True)
    key_cols = ['Code_company', 'CalcDate', 'ReportPeriod']
    assert not rtn[key_cols].isna().any().any()
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn = rtn[rtn['StatementType'] == '合并报表']
    rtn.drop(columns=['StatementType'], inplace=True)
    rtn = rtn[key_cols + flds]
    rtn.sort_values(by=['Code_company', 'CalcDate', 'ReportPeriod'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_ashare_financial_indicator(start_date, end_date, flds):
    path_financial_indicator = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\data\AShareFinancialIndicator.csv'
    name_dict = {'S_INFO_WINDCODE': 'Code', 'ANN_DT': 'CalcDate', 'REPORT_PERIOD': 'ReportPeriod', 'RD_EXPENSE': 'rd_expense', 'S_FA_DEDUCTEDPROFIT': 'profit'}
    rtn = pd.read_csv(path_financial_indicator, usecols=name_dict.keys(), dtype={'ANN_DT': str, 'REPORT_PERIOD': str})
    rtn.rename(columns=name_dict, inplace=True)
    key_cols = ['Code', 'CalcDate', 'ReportPeriod']
    rtn.dropna(subset=key_cols, inplace=True)
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    #
    report_quarter = rtn['ReportPeriod'].apply(func=lambda x: int(int(x[4:6])/3))
    report_year = rtn['ReportPeriod'].apply(func=lambda x: int(x[:4]))
    calc_year = rtn['CalcDate'].apply(func=lambda x: int(x[:4]))
    filter_cond_quarter123 = (calc_year == report_year) & (report_quarter != 4)
    filter_cond_quarter4 = (calc_year == report_year+1) & (report_quarter == 4)
    rtn = rtn[filter_cond_quarter123 | filter_cond_quarter4]
    #
    rtn = rtn[key_cols + flds]
    rtn = rtn[(rtn['CalcDate'] >= start_date) & (rtn['CalcDate'] <= end_date)]
    rtn.sort_values(by=['Code', 'CalcDate', 'ReportPeriod'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


load_ashare_financial_indicator('2020-01-01', '2021-01-0', [])


def load_mv_data(start_date, end_date, flds, freq):
    path_mv_data = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataUse\Stock_MVBPUse_2005to2021.csv'
    name_dict = {'S_INFO_WINDCODE': 'Code', 'TRADE_DT': 'CalcDate', 'S_VAL_MV': 'market_value', 'S_VAL_PE': 'pe', 'S_VAL_PB_NEW': 'pb_new', 'S_VAL_BP': 'bp'}
    rtn = pd.read_csv(path_mv_data, usecols=name_dict.keys(), dtype={'TRADE_DT': str})
    rtn.rename(columns=name_dict, inplace=True)
    key_cols = ['Code', 'CalcDate']
    assert not rtn[key_cols].isna().any().any()
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn = rtn[key_cols + flds]
    dates_for_adjust = pd.Series(pd.date_range(start=start_date, end=end_date, freq=freq))
    dates_for_adjust = dates_for_adjust.dt.strftime('%Y-%m-%d')
    rtn = rtn[rtn['CalcDate'].isin(dates_for_adjust)]
    rtn.sort_values(by=['Code', 'CalcDate'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_asharedescription():
    path_asharedescription = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\data\asharedescription_20210908.csv'
    name_dict = {'S_INFO_COMPCODE': 'Code_company', 'S_INFO_WINDCODE': 'Code'}
    asharedescription = pd.read_csv(path_asharedescription, usecols=name_dict.keys())
    asharedescription.rename(columns=name_dict, inplace=True)
    return asharedescription


def load_all_stock_basic_info(fields_description):
    path_asharedescription = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\data\asharedescription_20210916.csv'
    name_dict = {'WindCodes': 'Code', 'SEC_NAME': 'sec_name', 'IPO_DATE': 'ipo_date',
                 'INDUSTRY_CITIC': 'industry', 'PROVINCE': 'province'}
    rtn = pd.read_csv(path_asharedescription, usecols=name_dict.keys(), encoding='gbk')
    rtn.rename(columns=name_dict, inplace=True)
    # industry_detail_check = rtn[rtn['industry'] == '轻工制造'].sort_values(by='ipo_date').reset_index(drop=True)
    key_cols = ['Code']
    assert not rtn[key_cols].isna().any().any()
    rtn = rtn[key_cols + fields_description]
    if 'ipo_date' in fields_description:
        rtn['ipo_date'] = pd.to_datetime(rtn['ipo_date']).dt.strftime('%Y-%m-%d')
    return rtn


def load_ashare_daily_data(start_date, end_date, flds):
    path_daily_data1 = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\AShareEODPrices1.csv'
    path_daily_data2 = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\AShareEODPrices2.csv'
    name_dict = {'S_INFO_WINDCODE': 'Code', 'TRADE_DT': 'CalcDate', 'S_DQ_ADJCLOSE': 'close_adj'}
    rtn1 = pd.read_csv(path_daily_data1, usecols=name_dict.keys(), dtype={'TRADE_DT': str})
    rtn2 = pd.read_csv(path_daily_data2, usecols=name_dict.keys(), dtype={'TRADE_DT': str})
    rtn = pd.concat([rtn1, rtn2])
    rtn.rename(columns=name_dict, inplace=True)
    key_cols = ['Code', 'CalcDate']
    assert not rtn[key_cols].isna().any().any()
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    #
    rtn = rtn[key_cols + flds]
    rtn = rtn[(rtn['CalcDate'] >= start_date) & (rtn['CalcDate'] <= end_date)]
    rtn.sort_values(by=['Code', 'CalcDate'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_trade_days(start_date, end_date, read_type):
    if read_type == "sql":
        wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602', 'db': 'wind'}
        conn = pymysql.connect(charset='utf8', **wind_sql)
        trade_days = pd.read_sql('select * from asharecalendar where S_INFO_EXCHMARKET="SSE" order by TRADE_DAYS', con=conn)
        trade_days = trade_days["TRADE_DAYS"].to_list()
    elif read_type == "csv":
        path_trade_days = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\trade_days.csv'
        trade_days = pd.read_csv(path_trade_days)['TRADE_DT']
        trade_days = trade_days.to_list()
    else:
        assert False, "  error::wind_data>>load data>>read_type: {0} is unknown!".format(read_type)
    return trade_days


def load_industry_index_data(start_date, end_date, industry_citic):
    path_data = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\data\industry_index.xlsx'
    rtn = pd.read_excel(path_data, header=1)
    rtn.rename(columns=lambda x: x.replace('(中信)', ''), inplace=True)
    industrys_all = rtn.columns.to_list()
    industrys_use = ['医药', '机械', '电子', '汽车', '通信', '计算机', '基础化工', '电力设备及新能源']
    assert set(industrys_use).issubset(set(industrys_all))
    rtn.rename(columns={'时间': 'CalcDate', industry_citic: 'close'}, inplace=True)
    rtn['CalcDate'] = rtn['CalcDate'].dt.strftime('%Y-%m-%d')
    cols_use = ['CalcDate', 'close']
    assert not rtn[cols_use].isna().any().any()
    rtn = rtn[cols_use]
    rtn = rtn[(rtn['CalcDate'] >= start_date) & (rtn['CalcDate'] <= end_date)]
    rtn.sort_values(by=['CalcDate'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_patent_data_inventory(start_date, end_date, flds):
    path_patent_inventory1 = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\data\A股-有效存量专利指标数据（截至202102）.csv'
    rtn1 = pd.read_csv(path_patent_inventory1, encoding='gbk', dtype={'CPY_STOCK_CODE': str})
    path_patent_inventory2 = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\data\A股-有效存量专利指标数据（截至202102）2.csv'
    rtn2 = pd.read_csv(path_patent_inventory2, encoding='gbk', dtype={'CPY_STOCK_CODE': str})
    rtn = pd.concat([rtn1, rtn2])
    name_dict = {'CPY_NAME': 'CompName', 'CPY_STOCK_CODE': 'Code', 'DUE_DATE': 'CalcDate', 'TG_B001': 'pt_examing_num', 'TG_B002': 'pt_utility_num', 'TG_B003': 'pt_design_num', 'TG_B004': 'pt_authorized_num'}
    rtn.rename(columns=name_dict, inplace=True)
    rtn['Code'] = rtn['Code'].apply(func=lambda x: x + '.SH' if x[0] == '6' else x + '.SZ')
    key_cols = ['Code', 'CalcDate']
    assert not rtn[key_cols].isna().any().any()
    rtn = rtn[key_cols + flds]
    rtn = rtn[(rtn['CalcDate'] >= start_date) & (rtn['CalcDate'] <= end_date)]
    rtn.sort_values(by=['Code', 'CalcDate'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_patent_data_increase(start_date, end_date, flds):
    path_patent_inventory = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\data\A股_有效增量专利指标数据（截至202102）\A_有效增量专利指标'
    rtn = pd.DataFrame()
    for year in range(int(start_date[:4]), int(end_date[:4])+1):
        patent_data = pd.read_excel(path_patent_inventory+str(year)+'.xlsx', sheet_name='Sheet1', engine='openpyxl')
        rtn = rtn.append(patent_data)
    # name_dict = {'企业名称': 'CompName', '股票代码': 'Code', '截至日': 'CalcDate', 'TG1_B001': 'pt_examing_num_1Y', 'TG1_B002': 'pt_utility_num_1Y', 'TG1_B003': 'pt_design_num_1Y', 'TG1_B004': 'pt_authorized_num_1Y', 'TG2_B001': 'pt_examing_num_2Y', 'TG2_B002': 'pt_utility_num_2Y', 'TG2_B003': 'pt_design_num_2Y', 'TG2_B004': 'pt_authorized_num_2Y', 'TG3_B001': 'pt_examing_num_3Y', 'TG3_B002': 'pt_utility_num_3Y', 'TG3_B003': 'pt_design_num_3Y', 'TG3_B004': 'pt_authorized_num_3Y', 'TG4_B001': 'pt_examing_num_4Y', 'TG4_B002': 'pt_utility_num_4Y', 'TG4_B003': 'pt_design_num_4Y', 'TG4_B004': 'pt_authorized_num_4Y', 'TG5_B001': 'pt_examing_num_5Y', 'TG5_B002': 'pt_utility_num_5Y', 'TG5_B003': 'pt_design_num_5Y', 'TG5_B004': 'pt_authorized_num_5Y', 'TG3_B009': 'pt_examing_instruct_words_3Y', 'TG3_B012': 'pt_examing_claim_num_3Y', 'TG3_V001': 'pt_examing_claim_num_indep_3Y', 'TGi_B015': 'pt_examing_instruct_picts_3Y', 'TGi_B018': 'pt_examing_abst_words_3Y', 'TGi_B006': 'pt_examing_ipc_num_3Y', 'TGi_B021': 'pt_examing_life_3Y', 'TGi_B011': 'pt_authorized_instruct_words_3Y', 'TGi_B014': 'pt_authorized_claim_num_3Y', 'TGi_V003': 'pt_authorized_claim_num_indep_3Y', 'TGi_B017': 'pt_authorized_instruct_picts_3Y', '': ''}
    name_dict = {'企业名称': 'CompName', '股票代码': 'Code', '截止日': 'CalcDate'}
    rtn.rename(columns=name_dict, inplace=True)
    key_cols = ['Code', 'CalcDate']
    assert not rtn[key_cols].isna().any().any()
    rtn = rtn[key_cols + flds]
    rtn = rtn[(rtn['CalcDate'] >= start_date) & (rtn['CalcDate'] <= end_date)]
    rtn.sort_values(by=['Code', 'CalcDate'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_zz500_index_data(start_date, end_date):
    path_zz500 = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\CSISmallcap500Index.csv'
    name_dict = {'DateTime': 'CalcDate', 'WINDCODE': 'Code', 'CLOSE': 'close'}
    rtn = pd.read_csv(path_zz500, usecols=name_dict.keys(), encoding='gbk')
    rtn.rename(columns=name_dict, inplace=True)
    rtn['CalcDate'] = pd.to_datetime(rtn['CalcDate']).dt.strftime('%Y-%m-%d')
    cols_use = ['CalcDate', 'close']
    assert not rtn[cols_use].isna().any().any()
    rtn = rtn[cols_use]
    rtn = rtn[(rtn['CalcDate'] >= start_date) & (rtn['CalcDate'] <= end_date)]
    rtn.sort_values(by=['CalcDate'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn
