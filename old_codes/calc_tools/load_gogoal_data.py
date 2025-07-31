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


# load_data_from_gogoal("2020-01-01", "2021-12-30", ['report_type', 'forecast_np'], "csv")
