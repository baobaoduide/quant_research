import os
import pandas as pd
import platform
sys_platform = platform.platform().lower()
if 'macos' in sys_platform:
    path_data_local = r'/Volumes/Intern/database_local/stock'
    path_datalocal2 = r'/Volumes/Intern/database_local/stock/temp_predict_indexmem'
elif 'windows' in sys_platform:
    path_data_local = r'D:\database_local\stock'
    path_datalocal2 = r'D:\database_local\stock\temp_predict_indexmem'
else:
    print('其他系统')


def load_index_list():
    '''
    路径待修正
    '''
    name_dict = {'跟踪指数': 'code', '名称': 'name'}
    path_data = r'E:\NJU_term6\TF_Intern\predict_index\样本指数.xlsx'
    rtn = pd.read_excel(path_data, usecols=name_dict.keys())
    rtn.rename(columns=name_dict, inplace=True)
    return rtn


def load_aindexmembers(index_code):
    name_dict = {'S_INFO_WINDCODE': 'code_index', 'S_CON_WINDCODE': 'code', 'S_CON_INDATE': 'date_in', 'S_CON_OUTDATE': 'date_out'}
    path_data = os.path.join(path_data_local, 'AIndexMembers.csv')
    rtn = pd.read_csv(path_data, usecols=name_dict.keys(), dtype={'S_CON_INDATE': str, 'S_CON_OUTDATE': str})
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn['date_in'] = rtn['date_in'].apply(func=lambda x: x[:4]+'-'+x[4:6]+'-'+x[6:])
    rtn['date_out'] = rtn['date_out'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:] if isinstance(x, str) else None)
    rtn = rtn[rtn['code_index'] == index_code]
    rtn.drop(columns=['code_index'], inplace=True)
    rtn.sort_values(by=['code', 'date_in', 'date_out'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_asharedescription():
    name_dict = {'S_INFO_WINDCODE': 'code', 'S_INFO_NAME': 'name', 'S_INFO_LISTDATE': 'date_in',
                 'S_INFO_DELISTDATE': 'date_out', 'S_INFO_LISTBOARDNAME': 'list_board'}
    path_data = os.path.join(path_data_local, 'AShareDescription.csv')
    rtn = pd.read_csv(path_data, usecols=name_dict.keys(), dtype={'S_INFO_LISTDATE': str, 'S_INFO_DELISTDATE': str})
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn.dropna(subset=['date_in'], inplace=True)
    rtn['date_in'] = rtn['date_in'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
    rtn['date_out'] = rtn['date_out'].apply(
        func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:] if isinstance(x, str) else None)
    rtn.sort_values(by=['code', 'date_in'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_asharest():
    name_dict = {'S_INFO_WINDCODE': 'code', 'S_TYPE_ST': 'type', 'ENTRY_DT': 'date_in', 'REMOVE_DT': 'date_out'}
    path_data = os.path.join(path_data_local, 'AShareST.csv')
    rtn = pd.read_csv(path_data, encoding='gbk', usecols=name_dict.keys(), dtype={'ENTRY_DT': str, 'REMOVE_DT': str})
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn['date_in'] = rtn['date_in'].apply(
        func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
    rtn['date_out'] = rtn['date_out'].apply(
        func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:] if isinstance(x, str) else None)
    rtn.sort_values(by=['code'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_industry_cate_zz():
    name_dict = {'证券代码': 'code', '证券代码简称': 'name', '中证一级行业分类代码简称': 'industry_lv1',
       '中证二级行业分类代码简称': 'industry_lv2', '中证三级行业分类简称': 'industry_lv3', '中证四级行业分类简称': 'industry_lv4', '证监会行业门类简称': 'industry_csrc_m', '证监会行业种类简称': 'industry_csrc_z'}
    path_data = os.path.join(path_data_local, '中证行业分类.xlsx')
    rtn = pd.read_excel(path_data, usecols=name_dict.keys())
    rtn.rename(columns=name_dict, inplace=True)
    rtn['code'] = rtn['code'].apply(func=lambda x: str(x).zfill(6))
    rtn['code'] = rtn['code'].apply(func=lambda x: x + '.SH' if x[0] == '6' else x + '.SZ')
    return rtn


def load_ashareff():
    name_dict = {'S_INFO_WINDCODE': 'code', 'S_SHARE_FREESHARES': 'free_shares', 'ANN_DT': 'date', 'CHANGE_DT': 'change_dt', 'CHANGE_DT1': 'change_dt1'}
    path_data = os.path.join(path_data_local, 'AShareFreeFloat.csv')
    rtn = pd.read_csv(path_data, usecols=name_dict.keys(), dtype={'ANN_DT': str, 'CHANGE_DT': str, 'CHANGE_DT1': str})
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn.sort_values(by=['code', 'date', 'change_dt1'], inplace=True)
    # rtn['is_dup'] = rtn.duplicated(subset=['code', 'date'])
    rtn.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    rtn = rtn[['code', 'date', 'free_shares']]
    rtn['date'] = rtn['date'].apply(
        func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
    rtn.sort_values(by=['code', 'date'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_ashareeodprice(start_date, end_date):
    name_dict = {'S_INFO_WINDCODE': 'code', 'TRADE_DT': 'date', 'S_DQ_CLOSE': 'close', 'S_DQ_AMOUNT': 'amount'}
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
    rtn = rtn[rtn['date'].between(start_date, end_date)]
    return rtn


def load_asharemv(start_date, end_date):
    name_dict1 = {'S_INFO_WINDCODE': 'code', 'TRADE_DT': 'date', 'S_VAL_MV': 'mv'}
    path_data = os.path.join(path_data_local, 'Stock_MVBPUse.csv')
    rtn1 = pd.read_csv(path_data, usecols=name_dict1.keys(), dtype={'TRADE_DT': str})
    rtn1.rename(columns=name_dict1, inplace=True)
    name_dict2 = {'S_INFO_WINDCODE': 'code', 'TRADE_DT': 'date', 'S_DQ_TRADESTATUS': 'mv'}
    path_data = os.path.join(path_data_local, 'marketValue_to20220510.csv')
    rtn2 = pd.read_csv(path_data, dtype={'TRADE_DT': str})
    rtn2.rename(columns=name_dict2, inplace=True)
    rtn = pd.concat([rtn1, rtn2])
    rtn['mv'] = rtn['mv'] * 10000
    rtn.sort_values(by=['code', 'date'], inplace=True)
    rtn = rtn[rtn['date'].between(start_date, end_date)]
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_mv_temp(start_date, end_date):
    '''
    路径待修正
    '''
    path_data = r'H:\database_local\stocktemp_predict_indexmem\market_data_20150101to20220509.csv'
    rtn = pd.read_csv(path_data)
    rtn = rtn[rtn['date'].between(start_date, end_date)]
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_mv_rank(date):
    path_data = r'H:\database_local\stock\temp_predict_indexmem\mv_rank_exgem_to20210427.csv'
    rtn = pd.read_csv(path_data)
    rtn = rtn[rtn['date'] == date]
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_close_money_temp(start_date, end_date):
    path_data = r'H:\database_local\stock\temp_predict_indexmem\close_money_20150101to20220509.csv'
    rtn = pd.read_csv(path_data)
    rtn = rtn[rtn['date'].between(start_date, end_date)]
    rtn.sort_values(by=['code', 'date'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_ashareincome(stock_code, start_date, end_date):
    name_dict = {'S_INFO_WINDCODE': 'code', 'ACTUAL_ANN_DT': 'date', 'REPORT_PERIOD': 'report_period',
                 'STATEMENT_TYPE': 'statement_type', 'NET_PROFIT_EXCL_MIN_INT_INC': 'profit'}
    path_data = os.path.join(path_data_local, 'ashareincome_to20220510.csv')
    rtn = pd.read_csv(path_data, encoding='gbk', usecols=name_dict.keys(), dtype={'ACTUAL_ANN_DT': str, 'REPORT_PERIOD': str})
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn = rtn[rtn['statement_type'].isin([408001000, 408005000, 408027000, 408028000])]
    if isinstance(stock_code, str):
        rtn = rtn[rtn['code'] == stock_code]
        rtn.drop(columns=['code'], inplace=True)
        rtn.sort_values(by=['date', 'report_period'], inplace=True)
    elif isinstance(stock_code, list):
        rtn = rtn[rtn['code'].isin(stock_code)]
        rtn.sort_values(by=['code', 'date', 'report_period'], inplace=True)
    rtn['date'] = rtn['date'].apply(
        func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8] if isinstance(x, str) else None)
    rtn.dropna(subset=['date'], inplace=True)
    print(rtn['date'].max(), rtn['date'].min())  # 2021-12-31 1990-03-21
    rtn = rtn[rtn['date'].between(start_date, end_date)]
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_asharebalancesheet(stock_code, start_date, end_date):
    name_dict = {'S_INFO_WINDCODE': 'code', 'ACTUAL_ANN_DT': 'date', 'REPORT_PERIOD': 'report_period',
                 'STATEMENT_TYPE': 'statement_type', 'TOT_SHRHLDR_EQY_EXCL_MIN_INT': 'net_asset', 'TOT_LIAB': 'tot_liab', 'TOT_ASSETS': 'tot_assets'}
    path_data = os.path.join(path_data_local, 'asharebalancesheet_to20220510.csv')
    rtn = pd.read_csv(path_data, usecols=name_dict.keys(), dtype={'REPORT_PERIOD': str, 'ACTUAL_ANN_DT': str})
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn = rtn[rtn['statement_type'].isin([408001000, 408005000, 408027000, 408028000])]
    if isinstance(stock_code, str):
        rtn = rtn[rtn['code'] == stock_code]
        rtn.drop(columns=['code'], inplace=True)
        rtn.sort_values(by=['date', 'report_period'], inplace=True)
    elif isinstance(stock_code, list):
        rtn = rtn[rtn['code'].isin(stock_code)]
        rtn.sort_values(by=['code', 'date', 'report_period'], inplace=True)
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8] if isinstance(x, str) else None)
    rtn.dropna(subset=['date'], inplace=True)
    print(rtn['date'].max(), rtn['date'].min())  # 2021-12-31 1990-03-21
    rtn = rtn[rtn['date'].between(start_date, end_date)]
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_asharedividend(stock_code, start_date, end_date):
    name_dict = {'S_INFO_WINDCODE': 'code', 'EX_DT': 'date', 'REPORT_PERIOD': 'report_period',
                 'CASH_DVD_PER_SH_PRE_TAX': 'cash_div_ps_pre', 'CASH_DVD_PER_SH_AFTER_TAX': 'cash_div_ps_aft',
                 'S_DIV_BASESHARE': 'base_share'}
    path_data = os.path.join(path_data_local, 'AShareDividend_to20220510.csv')
    rtn = pd.read_csv(path_data, usecols=name_dict.keys(), dtype={'REPORT_PERIOD': str, 'EX_DT': str})
    rtn.rename(columns=name_dict, inplace=True)
    rtn = rtn[rtn['date'].between(start_date, end_date)]
    if isinstance(stock_code, str):
        rtn = rtn[rtn['code'] == stock_code]
        rtn.drop(columns=['code'], inplace=True)
        rtn.sort_values(by=['date', 'report_period'], inplace=True)
    elif isinstance(stock_code, list):
        rtn = rtn[rtn['code'].isin(stock_code)]
        rtn.sort_values(by=['code', 'date', 'report_period'], inplace=True)
    rtn.dropna(subset=['date'], inplace=True)
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:] if isinstance(x, str) else None)
    print(rtn['date'].max(), rtn['date'].min())  # 2021-12-15 1991-05-02
    rtn['base_share'] = rtn['base_share'] * 10000
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_tradedays():
    path_data = r'H:\database_local\stock\temp_predict_indexmem\tradedays.xlsx'
    rtn = pd.read_excel(path_data, header=None)[0].dt.strftime('%Y-%m-%d')
    return rtn


def load_asharetotal():
    name_dict = {'S_INFO_WINDCODE': 'code', 'CHANGE_DT': 'date', 'S_SHARE_TOTALA': 'share_total'}
    path_data = r'H:\database_local\stock\temp_predict_indexmem\asharetotalA.csv'
    rtn = pd.read_csv(path_data, dtype={'CHANGE_DT': str})
    rtn.rename(columns=name_dict, inplace=True)
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:8])
    return rtn


def adjust_code(num_code):
    num_code_str = str(num_code).zfill(6)
    if num_code_str[0] in ['4', '8']:
        code = num_code_str + '.BJ'
    elif num_code_str[0] == '6':
        code = num_code_str + '.SH'
    else:
        code = num_code_str + '.SZ'
    return code


def load_stockinfo_useful():
    name_dict = {'windcode': 'code', 'trade_dt': 'date'}
    path_data = r'H:\database_local\stock\temp_predict_indexmem\stock_info_useful.csv'
    rtn = pd.read_csv(path_data)
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn['code'] = rtn['code'].apply(adjust_code)
    rtn['date'] = rtn['date'].astype(str)
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4]+'-'+x[4:6]+'-'+x[6:8])
    return rtn


def load_result():
    path_data = r'H:\database_local\stock\temp_predict_indexmem\202206重点指数成分股调整预测.xlsx'
    rtn = pd.read_excel(path_data, sheet_name='上证180', header=1)
    return rtn


def load_main_business():
    path_data = os.path.join(path_datalocal2, '全市场个股主营业务数据.xlsx')
    rtn = pd.read_excel(path_data)
    name_dict = {'证券代码': 'code', '证券简称': 'name', '主营产品名称': 'main_product', '主营产品类型': 'main_product_type'}
    rtn.rename(columns=name_dict, inplace=True)
    return rtn


def load_profit_data():
    path_data = r'H:\database_local\stock\temp_predict_indexmem\profit_last_year.csv'
    rtn = pd.read_csv(path_data, encoding='gbk')
    rtn = rtn[['code', 'profit']]
    return rtn


def load_cate_info_data():
    path_data = r'H:\database_local\stock\temp_predict_indexmem\分类信息汇总.xlsx'
    rtn = pd.read_excel(path_data)
    name_dict = {'证券代码': 'code', '证券简称': 'name', '上市日期': 'date_in', '所属概念板块\n[交易日期] 最新收盘日': 'board_gn', '所属热门概念\n[交易日期] 最新收盘日': 'board_gn_hot', '所属产业链板块': 'board_industry', '公司简介': 'comp_brief', '主营产品名称': 'main_product', '主营产品类型': 'main_product_type'}
    rtn.rename(columns=name_dict, inplace=True)
    return rtn


def load_wind_industry():
    path_folder = r'H:\database_local\stock\temp_predict_indexmem\板块分类信息'
    rtn = pd.read_excel(os.path.join(path_folder, '万德一级行业分类.xlsx'))
    name_dict = {'证券代码': 'code', '证券简称': 'name', '所属Wind行业名称\n[行业级别] 一级行业': 'ind_wind1', '所属Wind行业名称\n[行业级别] 二级行业': 'ind_wind2',
       '所属Wind行业名称\n[行业级别] 三级行业': 'ind_wind3', '所属Wind行业名称\n[行业级别] 四级行业': 'ind_wind4'}
    rtn.rename(columns=name_dict, inplace=True)
    return rtn


def load_ashare_financial_indicator(start_date, end_date, flds):
    path_financial_indicator = r'E:\database_local\stock\AShareFinancialIndicator.csv'
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
    # rtn = rtn[key_cols + flds]
    rtn = rtn[(rtn['CalcDate'] >= start_date) & (rtn['CalcDate'] <= end_date)]
    rtn.sort_values(by=['Code', 'CalcDate', 'ReportPeriod'], inplace=True)
    rtn.drop_duplicates(inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


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
    if read_type == "csv":
        cols_use = ['report_id', 'create_date', 'organ_name', 'organ_id', 'title', 'stock_code', 'report_year', 'report_quarter'] + flds
        # path_data = os.path.join(os.path.dirname(__file__), 'rpt_forecast_stk')
        path_data = r"/Volumes/Intern/database_local/analyst/rpt_forecast_stk/rpt_forecast_stk"
        path_data = r"E:\database_local\analyst\rpt_forecast_stk\rpt_forecast_stk"
        # path_data = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\rpt_forecast_stk\rpt_forecast_stk'
        rtn_list = []
        for year in range(int(start_date[:4]), int(end_date[:4])+1):
            forecast_data_i = pd.read_csv(path_data+"_"+str(year)+".csv", usecols=cols_use, dtype={'stock_code': str})
            rtn_list.append(forecast_data_i)
        rtn = pd.concat(rtn_list)
        rtn.rename(columns={'stock_code': 'Code'}, inplace=True)
        rtn = rtn[rtn["create_date"].between(start_date, end_date, inclusive="both")]
    else:
        assert False, "  error::gogoal_data>>load data>>read_type: {0} is unknown!".format(read_type)
    # validate
    rtn = patch_data(rtn)
    key_cols = ['create_date', 'organ_name', 'title', 'Code', 'report_year', 'report_quarter']
    assert rtn[rtn.duplicated(key_cols)].empty
    rtn[['report_year', 'report_quarter']] = rtn[['report_year', 'report_quarter']].fillna(-1).astype(int)
    # assert not rtn[key_cols].isna().any().any()
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


if __name__ == '__main__':
    all_flds = [
        "report_type",
        "author_name",
        "stock_name",
        "forecast_np",
        "forecast_eps",
        "forecast_dps",
        "forecast_pe",
        "forecast_roe",
    ]
    load_data_from_gogoal('2020-01-01', '2021-04-30', all_flds, 'csv')
    # load_aindexmembers('000300.SH')
    # stock_basic_info = load_asharedescription()
    # st_info = load_asharest()
    # industry_cate = load_industry_cate_zz()
    # ashare_freefloat = load_ashareff()
    # stock_money = load_ashareeodprice()
    stock_mv = load_asharemv('1990-12-19', '2023-01-01')
    # codes = load_asharedescription()["code"].to_list()
    # load_ashareincome(codes, '1990-01-01', '2022-05-08')
    # load_asharebalancesheet([], '1990-01-01', '2022-05-08')
    # load_asharedividend([], '1990-01-01', '2022-05-08')
    # load_tradedays()
    # load_asharetotal()
    # load_result()
    # load_cate_info_data()
    # load_profit_data()
    # load_main_business()
    # st_data = load_asharest()
    # load_stockinfo_useful()
    load_wind_industry()
