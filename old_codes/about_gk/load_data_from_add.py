import os
import pandas as pd
import platform
sys_platform = platform.platform().lower()
if 'macos' in sys_platform:
    path_data_local = r'/Users/xiaotianyu/Desktop/data/raw_data_add'
elif 'windows' in sys_platform:
    path_data_local = r'D:\database_local\data_gk_model\raw_data_add'
else:
    print('其他系统')


def load_aindexmembers(index_code):
    name_dict = {'S_INFO_WINDCODE': 'code_index', 'S_CON_WINDCODE': 'code', 'S_CON_INDATE': 'date_in', 'S_CON_OUTDATE': 'date_out'}
    path_data = os.path.join(path_data_local, '中国A股指数成份股[AIndexMembers].csv')
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


def load_aindexmemberscitics(index_code):
    name_dict = {'S_INFO_WINDCODE': 'code_index', 'S_CON_WINDCODE': 'code', 'S_CON_INDATE': 'date_in', 'S_CON_OUTDATE': 'date_out'}
    path_data = os.path.join(path_data_local, '中国A股中信指数成份股[AIndexMembersCITICS].csv')
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
    path_data = os.path.join(path_data_local, '中国A股基本资料[AShareDescription].csv')
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


def load_asharedividend(stock_code, start_date, end_date):
    name_dict = {'S_INFO_WINDCODE': 'code', 'EX_DT': 'date', 'REPORT_PERIOD': 'report_period',
                 'CASH_DVD_PER_SH_PRE_TAX': 'cash_div_ps_pre', 'CASH_DVD_PER_SH_AFTER_TAX': 'cash_div_ps_aft',
                 'S_DIV_BASESHARE': 'base_share'}
    path_data = os.path.join(path_data_local, '中国A股分红[AShareDividend].csv')
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
    print(rtn['date'].max(), rtn['date'].min())
    rtn['base_share'] = rtn['base_share'] * 10000
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_ashareincome(stock_code, start_date, end_date):
    name_dict = {'S_INFO_WINDCODE': 'code', 'ACTUAL_ANN_DT': 'date', 'REPORT_PERIOD': 'report_period',
                 'STATEMENT_TYPE': 'statement_type', 'NET_PROFIT_EXCL_MIN_INT_INC': 'profit'}
    path_data = os.path.join(path_data_local, '中国A股利润表[AShareIncome].csv')
    rtn = pd.read_csv(path_data, usecols=name_dict.keys(), dtype={'ACTUAL_ANN_DT': str, 'REPORT_PERIOD': str})
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


def load_AShareEODDerivativeIndicator(start_date, end_date):
    path_data = os.path.join(path_data_local, '中国A股日行情估值指标[AShareEODDerivativeIndicator].csv')
    rtn = pd.read_csv(path_data,
                      usecols=['S_INFO_WINDCODE', 'TRADE_DT', 'S_VAL_MV', 'TOT_SHR_TODAY'], dtype={'TRADE_DT': str})
    name_dict = {'S_INFO_WINDCODE': 'code', 'TRADE_DT': 'date', 'S_VAL_MV': 'mv', 'TOT_SHR_TODAY': 'share_total'}
    rtn.rename(columns=name_dict, inplace=True)
    #
    rtn['date'] = rtn['date'].apply(func=lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
    rtn['mv'] = rtn['mv'] * 10000
    rtn['share_total'] = rtn['share_total'] * 10000
    rtn = rtn[rtn["date"].between(start_date, end_date)]
    rtn.sort_values(by=['code', 'date'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def load_asharebalancesheet2(stock_code, start_date, end_date):
    name_dict = {'S_INFO_WINDCODE': 'code', 'ACTUAL_ANN_DT': 'date', 'REPORT_PERIOD': 'report_period',
                 'STATEMENT_TYPE': 'statement_type', 'TOT_SHRHLDR_EQY_EXCL_MIN_INT': 'net_asset', 'TOT_LIAB': 'tot_liab', 'TOT_ASSETS': 'tot_assets'}
    name_dict_new = {key.lower(): value for key, value in name_dict.items()}
    path_data = os.path.join(path_data_local, 'asharebalancesheet.csv')
    rtn = pd.read_csv(path_data, usecols=name_dict_new.keys(), dtype={'report_period': str, 'actual_ann_dt': str})
    rtn.rename(columns=name_dict_new, inplace=True)
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


def patch_data(data):
    organ_num = data.groupby(["report_id"])["organ_name"].apply(lambda x: x.unique().shape[0])
    organ_error_list = list(organ_num[organ_num > 1].index)
    rtn = data[~data["report_id"].isin(organ_error_list)].copy()
    print("warning::朝阳永续数据中存在同一篇报告数据对应多个机构名（券商旧名与新名）的情况，占比约千分之一，此处暂时剔除")
    rtn.drop(columns=["organ_id"], inplace=True)
    return rtn


def load_data_from_gogoal2(start_date, end_date, flds, read_type):
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
        path_data = os.path.join(path_data_local, 'rpt_forecast_stk.csv')
        rtn = pd.read_csv(path_data, usecols=cols_use, dtype={'stock_code': str})
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
    load_aindexmemberscitics('CI005023.WI')
    # load_aindexmembers("")
    all_codes = load_asharedescription()['code'].to_list()
    # load_ashareincome(all_codes, '1990-01-01', '2023-08-20')
    load_asharedividend(all_codes, '1990-01-01', '2023-08-20')
    # load_AShareEODDerivativeIndicator('2008-01-01', '2023-08-18')
    # data = load_asharebalancesheet(all_codes, '1990-01-01', '2023-08-18') # 2022-01-01开始+
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
    load_data_from_gogoal2('1990-01-01', '2023-08-18', all_flds, 'csv') # 2017-03-24开始的数据
