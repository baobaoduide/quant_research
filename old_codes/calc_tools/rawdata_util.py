import os
import pandas as pd
import pymysql


def patch_data(data):
    organ_num = data.groupby(["report_id"])["organ_name"].apply(lambda x: x.unique().shape[0])
    organ_error_list = list(organ_num[organ_num > 1].index)
    rtn = data[~data["report_id"].isin(organ_error_list)].copy()
    print("warning::朝阳永续数据中存在同一篇报告数据对应多个机构名（券商旧名与新名）的情况，占比约千分之一，此处暂时剔除")
    rtn.drop(columns=["organ_id"], inplace=True)
    return rtn


def load_data_from_gogoal(start_date, end_date, flds, read_type):
    assert isinstance(flds, list)
    all_flds = ['content', 'report_type', 'author_name', 'stock_name',
                'forecast_or', 'forecast_op', 'forecast_tp', 'forecast_np', 'forecast_eps',
                'forecast_dps', 'forecast_rd', 'forecast_pe', 'forecast_roe',
                'forecast_ev_ebitda', 'organ_rating_content',
                'target_price_ceiling', 'target_price_floor', 'current_price', 'refered_capital',
                'is_capital_change', 'settlement_date']
    assert set(flds).issubset(all_flds)
    if read_type == "sql":
        gogoal_sql = {'host': '192.168.41.62', 'port': 3306, 'user': 'cusreader', 'passwd': 'cus_reader@tf1603',
                      'db': 'cus_fund_db2'}
        conn = pymysql.connect(charset='utf8', **gogoal_sql)
        rtn = pd.read_sql("select report_id,create_date,organ_name,organ_id,title,stock_code Code,report_year,report_quarter,{0} "
                          "from rpt_forecast_stk "
                          "where create_date between '{1}' and '{2}' order by create_date".format(
                           ",".join(flds), start_date, end_date), con=conn)
        rtn["create_date"] = rtn["create_date"].astype(str)
    elif read_type == "csv":
        cols_use = ['report_id', 'create_date', 'organ_name', 'organ_id', 'title', 'stock_code', 'report_year', 'report_quarter'] + flds
        # path_data = os.path.join(os.path.dirname(__file__), 'rpt_forecast_stk')
        path_data = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\rpt_forecast_stk\rpt_forecast_stk'
        rtn = pd.DataFrame()
        for year in range(int(start_date[:4]), int(end_date[:4])+1):
            forecast_data_i = pd.read_csv(path_data+"_"+str(year)+".csv", usecols=cols_use, dtype={'stock_code': str})
            rtn = rtn.append(forecast_data_i)
        rtn.rename(columns={'stock_code': 'Code'}, inplace=True)
        rtn = rtn[rtn["create_date"].between(start_date, end_date, inclusive="both")]
    else:
        assert False, "  error::gogoal_data>>load data>>read_type: {0} is unknown!".format(read_type)
    # validate
    rtn = patch_data(rtn)
    key_cols = ['create_date', 'organ_name', 'title', 'Code', 'report_year', 'report_quarter']
    assert rtn[rtn.duplicated(key_cols)].empty
    rtn[['report_year', 'report_quarter']] = rtn[['report_year', 'report_quarter']].fillna(-1).astype(int)
    assert not rtn[key_cols].isna().any().any()
    pd.testing.assert_frame_equal(rtn[['create_date', 'organ_name', 'title', 'Code', 'report_id']
                                  ].drop_duplicates(["report_id"], keep="first"),
                                  rtn[['create_date', 'organ_name', 'title', 'Code', 'report_id']
                                  ].drop_duplicates(['create_date', 'organ_name', 'title', 'Code'], keep="first"))
    rtn.drop(columns=["report_id"], inplace=True)
    rtn['Code'] = rtn['Code'].apply(func=lambda x: x + '.SH' if x[0] == '6' else x + '.SZ')
    num_flds = list(set(flds) & {'forecast_or', 'forecast_op', 'forecast_tp', 'forecast_np', 'refered_capital'})
    if num_flds:
        rtn[num_flds] = rtn[num_flds] * 10000.0
    percentage_flds = list(set(flds) & {"forecast_rd", "forecast_roe"})
    if percentage_flds:
        rtn[percentage_flds] = rtn[percentage_flds] / 100.0
    if "report_type" in flds:
        type_dict = {"21": "非个股报告", "22": "一般个股报告", "23": "深度报告", "24": "调研报告", "25": "点评报告",
                     "26": "新股研究", "27": "简评文章", "28": "港股研究", "98": "会议纪要", "125": "科创板新股研究"}
        rtn["report_type"] = rtn["report_type"].apply(lambda x: type_dict[str(x)])
    rtn.reset_index(drop=True, inplace=True)
    return rtn


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


def load_analyst_match(start_date, end_date):
    # path_analyst_match = os.path.join(os.path.dirname(__file__), 'analyst_match_2019to2020.csv')
    path_analyst_match = r'E:\NJU_term4\TF_Intern\EngineerWork\analyst_related_record\analyst_match_2019to2020.csv'
    analyst_match = pd.read_csv(path_analyst_match)
    analyst_match = analyst_match[(analyst_match['create_date'] >= start_date) & (analyst_match['create_date'] <= end_date)]
    return analyst_match


# temp = load_data_from_gogoal("2020-01-01", "2020-12-30", ['forecast_np'], "csv")
# load_data_from_wind_income("2020-01-01", "2020-12-30", ['NetProfit'], "csv")
# load_data_from_wind_balance("2020-01-01", "2020-12-30", ['total_asset'], "csv")
# load_data_from_wind_express("2020-01-01", "2020-12-30", ['NetProfit'], "csv")
# load_data_from_wind_notice("2020-01-01", "2020-12-30", [],"csv")
# load_trade_days("2020-01-01", "2020-12-30", 'csv')
