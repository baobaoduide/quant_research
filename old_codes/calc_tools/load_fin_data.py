import os
import pandas as pd
import pymysql


def load_data_from_wind_income(start_date, end_date, flds, read_type="sql"):
    assert isinstance(flds, list)
    if read_type == "sql":
        name_dict = {'NetProfit': 'NET_PROFIT_EXCL_MIN_INT_INC', 'revenue': 'TOT_OPER_REV'}
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
                     'TOT_OPER_REV': 'revenue'}
        # path_income = os.path.join(os.path.dirname(__file__), 'AShareIncome.csv')
        path_income = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\AShareIncome.csv'
        rtn = pd.read_csv(path_income, usecols=name_dict.keys(), dtype={'ACTUAL_ANN_DT': str, 'REPORT_PERIOD': str})
        rtn.rename(columns=name_dict, inplace=True)
        rtn = rtn[['Code', 'CalcDate', 'ReportPeriod', 'StatementType'] + flds]
        rtn = rtn[rtn['CalcDate'].between(start_date.replace("-", ""), end_date.replace("-", ""), inclusive='both')]
        rtn['StatementType'] = rtn['StatementType'].astype(str)
    else:
        assert False, "  error::gogoal_data>>load data>>read_type: {0} is unknown!".format(read_type)
    rtn.sort_values(by=['Code', 'CalcDate', 'ReportPeriod'], inplace=True)
    key_cols = ['Code', 'CalcDate', 'ReportPeriod']
    assert not rtn[key_cols].isna().any().any()
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn = rtn[rtn['StatementType'].isin(["408001000", "408004000", "408005000", "408027000", "408028000", "	408029000", "408050000"])]
    type_level_dict = {"408001000": 1, "408004000": 2, "408005000": 3, "408027000": 5, "408028000": 4,
                       "408029000": 7, "408050000": 6}
    rtn["优先级"] = rtn["StatementType"].apply(lambda x: type_level_dict[x])
    rtn.sort_values(["CalcDate", "Code", "ReportPeriod", "优先级"], ascending=True, inplace=True)
    rtn.drop_duplicates(["CalcDate", "Code", "ReportPeriod"], keep="first", inplace=True)
    rtn.drop(columns=['StatementType', '优先级'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_data_from_wind_balance(start_date, end_date, flds, read_type):
    assert isinstance(flds, list)
    if read_type == 'sql':
        name_dict = {'equity': 'TOT_SHRHLDR_EQY_EXCL_MIN_INT'}
        assert set(flds).issubset(name_dict.values())
        wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602', 'db': 'wind'}
        conn = pymysql.connect(charset='utf8', **wind_sql)
        if flds:
            cols_use = ",".join([name_dict[i] + " " + i for i in flds])
            rtn = pd.read_sql("select S_INFO_WINDCODE Code, ACTUAL_ANN_DT CalcDate, REPORT_PERIOD ReportPeriod,{0} "
                      "from asharebalancesheet where ACTUAL_ANN_DT between {1} and {2}".format(
                        cols_use, start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
        else:
            rtn = pd.read_sql("select S_INFO_WINDCODE Code, ACTUAL_ANN_DT CalcDate, REPORT_PERIOD ReportPeriod "
                              "from asharebalancesheet where ACTUAL_ANN_DT between {0} and {1}".format(start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
    elif read_type == 'csv':
        name_dict = {'S_INFO_WINDCODE': 'Code', 'ACTUAL_ANN_DT': 'CalcDate', 'REPORT_PERIOD': 'ReportPeriod',
                     'STATEMENT_TYPE': 'StatementType', 'TOT_ASSETS': 'total_asset',
                     'TOT_SHRHLDR_EQY_EXCL_MIN_INT': 'equity'}
        # path_balance = os.path.join(os.path.dirname(__file__), 'AShareIncome.csv')
        path_balance = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\AShareBalanceSheet.csv'
        rtn = pd.read_csv(path_balance, usecols=name_dict.keys(), dtype={'ACTUAL_ANN_DT': str, 'REPORT_PERIOD': str})
        rtn.rename(columns=name_dict, inplace=True)
        rtn = rtn[['Code', 'CalcDate', 'ReportPeriod', 'StatementType'] + flds]
        rtn = rtn[rtn['CalcDate'].between(start_date.replace("-", ""), end_date.replace("-", ""), inclusive='both')]
        rtn['StatementType'] = rtn['StatementType'].astype(str)
    else:
        assert False, "  error::wind_data>>load data>>read_type: {0} is unknown!".format(read_type)
    rtn.sort_values(by=['Code', 'CalcDate', 'ReportPeriod'], inplace=True)
    key_cols = ['Code', 'CalcDate', 'ReportPeriod']
    assert not rtn[key_cols].isna().any().any()
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn = rtn[rtn['StatementType'].isin(["408001000", "408004000", "408005000", "408027000", "408028000", "	408029000", "408050000"])]
    type_level_dict = {"408001000": 1, "408004000": 2, "408005000": 3, "408027000": 5, "408028000": 4,
                       "408029000": 7, "408050000": 6}
    rtn["优先级"] = rtn["StatementType"].apply(lambda x: type_level_dict[x])
    rtn.sort_values(["CalcDate", "Code", "ReportPeriod", "优先级"], ascending=True, inplace=True)
    rtn.drop_duplicates(["CalcDate", "Code", "ReportPeriod"], keep="first", inplace=True)
    rtn.drop(columns=['StatementType', '优先级'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_data_from_wind_express(start_date, end_date, flds, read_type="sql"):
    assert isinstance(flds, list)
    if read_type == "sql":
        name_dict = {'NetProfit': 'NET_PROFIT_EXCL_MIN_INT_INC', 'revenue': 'OPER_REV',
                     'equity': 'TOT_SHRHLDR_EQY_EXCL_MIN_INT'}
        assert set(flds).issubset(name_dict.values())
        wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602',
                    'db': 'wind'}
        conn = pymysql.connect(charset='utf8', **wind_sql)
        if flds:
            cols_use = ",".join([name_dict[i] + " " + i for i in flds])
            rtn = pd.read_sql("select S_INFO_WINDCODE Code,ACTUAL_ANN_DT CalcDate,REPORT_PERIOD ReportPeriod,{0} "
                              "from ashareprofitexpress where ACTUAL_ANN_DT between {1} and {2}".format(
                                cols_use, start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
        else:
            rtn = pd.read_sql("select S_INFO_WINDCODE Code,ACTUAL_ANN_DT CalcDate,REPORT_PERIOD ReportPeriod "
                              "from ashareprofitexpress where ACTUAL_ANN_DT between {0} and {1}".format(
                               start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
    elif read_type == "csv":
        name_dict = {'S_INFO_WINDCODE': 'Code', 'ANN_DT': 'CalcDate', 'REPORT_PERIOD': 'ReportPeriod',
                     'NET_PROFIT_EXCL_MIN_INT_INC': 'NetProfit', 'OPER_REV': 'revenue',
                     'TOT_SHRHLDR_EQY_EXCL_MIN_INT': 'equity'}
        path_express = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\AShareProfitExpress.csv'
        rtn = pd.read_csv(path_express, usecols=name_dict.keys(), dtype={'ANN_DT': str, 'REPORT_PERIOD': str})
        rtn.rename(columns=name_dict, inplace=True)
        rtn = rtn[['Code', 'CalcDate', 'ReportPeriod'] + flds]
        rtn = rtn[rtn['CalcDate'].between(start_date.replace("-", ""), end_date.replace("-", ""), inclusive='both')]
    else:
        assert False, "  error::wind_data>>load data>>read_type: {0} is unknown!".format(read_type)
    rtn.sort_values(by=['Code', 'CalcDate', 'ReportPeriod'], inplace=True)
    key_cols = ['Code', 'CalcDate', 'ReportPeriod']
    assert not rtn[key_cols].isna().any().any()
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_data_from_wind_notice(start_date, end_date, flds, read_type):
    assert isinstance(flds, list)
    if read_type == "sql":
        name_dict = {'ProfitChange_Min': 'S_PROFITNOTICE_CHANGEMIN', 'ProfitChange_Max': 'S_PROFITNOTICE_CHANGEMAX', 'Profit_Min': 'S_PROFITNOTICE_NETPROFITMIN', 'Profit_Max': 'S_PROFITNOTICE_NETPROFITMAX'}
        assert set(flds).issubset(name_dict.values())
        wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602', 'db': 'wind'}
        conn = pymysql.connect(charset='utf8', **wind_sql)
        if flds:
            cols_use = ",".join([name_dict[i] + " " + i for i in flds])
            rtn = pd.read_sql("select S_INFO_WINDCODE Code,S_PROFITNOTICE_DATE CalcDate,S_PROFITNOTICE_PERIOD ReportPeriod,{0} from ashareprofitnotice where S_PROFITNOTICE_DATE between {1} and {2}".format(cols_use, start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
        else:
            rtn = pd.read_sql("select S_INFO_WINDCODE Code,S_PROFITNOTICE_DATE CalcDate,S_PROFITNOTICE_PERIOD ReportPeriod from ashareprofitnotice where S_PROFITNOTICE_DATE between {0} and {1}".format(start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
    elif read_type == "csv":
        name_dict = {'S_INFO_WINDCODE': 'Code', 'S_PROFITNOTICE_DATE': 'CalcDate', 'S_PROFITNOTICE_PERIOD': 'ReportPeriod', 'S_PROFITNOTICE_CHANGEMIN': 'ProfitChange_Min', 'S_PROFITNOTICE_CHANGEMAX': 'ProfitChange_Max', 'S_PROFITNOTICE_NETPROFITMIN': 'Profit_Min', 'S_PROFITNOTICE_NETPROFITMAX': 'Profit_Max'}
        # path_notice = os.path.join(os.path.dirname(__file__), 'AShareProfitNotice.csv')
        path_notice = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\AShareProfitNotice.csv'
        rtn = pd.read_csv(path_notice, usecols=name_dict.keys(), dtype={'S_PROFITNOTICE_DATE': str, 'S_PROFITNOTICE_PERIOD': str})
        rtn.rename(columns=name_dict, inplace=True)
        rtn = rtn[['Code', 'CalcDate', 'ReportPeriod'] + flds]
        rtn = rtn[rtn['CalcDate'].between(start_date.replace("-", ""), end_date.replace("-", ""), inclusive='both')]
    else:
        assert False, "  error::wind_data>>load data>>read_type: {0} is unknown!".format(read_type)
    num_flds = list(set(flds) & {'Profit_Min', 'Profit_Max'})
    if num_flds:
        rtn[num_flds] = rtn[num_flds] * 10000.0
    percentage_flds = list(set(flds) & {"ProfitChange_Min", "ProfitChange_Max"})
    if percentage_flds:
        rtn[percentage_flds] = rtn[percentage_flds] / 100.0
    rtn.sort_values(by=['Code', 'CalcDate', 'ReportPeriod'], inplace=True)
    key_cols = ['Code', 'CalcDate', 'ReportPeriod']
    assert not rtn[key_cols].isna().any().any()
    rtn['CalcDate'] = rtn['CalcDate'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
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


def load_ipo_data(start_date, end_date):
    wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602', 'db': 'wind'}
    conn = pymysql.connect(charset='utf8', **wind_sql)
    ashareipo = pd.read_sql("select S_INFO_WINDCODE Code,ANN_DT CalcDate from ashareipo "
                            "where ANN_DT between {0} and {1}".format(
                                start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
    ashareipo["CalcDate"] = ashareipo["CalcDate"].astype(str).apply(lambda x: x[:4]+"-"+x[4:6]+"-"+x[6:8])
    return ashareipo


# load_ipo_data("20160101", "20181231")
# load_data_from_wind_income("2020-01-01", "2020-12-30", ['NetProfit'], "csv")
# load_data_from_wind_balance("2020-01-01", "2020-12-30", ['equity'], "csv")
# load_data_from_wind_express("2020-01-01", "2020-12-30", ['equity'], "csv")
# load_data_from_wind_notice("2020-01-01", "2020-12-30", ['ProfitChange_Min'], "csv")
# load_trade_days("2020-01-01", "2020-12-30", "csv")
