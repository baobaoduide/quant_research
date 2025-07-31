import pymysql
import pandas as pd


def load_data_from_wind_eodprices(start_date, end_date, fields):
    wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602',
                'db': 'wind'}
    conn = pymysql.connect(charset='utf8', **wind_sql)
    cols_use = ",".join(fields)
    # sql = "select S_INFO_WINDCODE code,TRADE_DT date,{0} " \
    #       "from AShareEODPrices where TRADE_DT between {1} and {2}".format(cols_use, start_date, end_date)
    sql = "select * " \
          "from AShareEODPrices where TRADE_DT between {0} and {1}".format(start_date, end_date)
    rtn = pd.read_sql(sql, con=conn)
    rtn.sort_values(by=['code', 'date'], inplace=True)
    rtn.reset_index(drop=True, inplace=True)
    return rtn


def cal_func(code_data):
    code_data['nv'] = (code_data['ret'] + 1).cumprod()

    code_data_tbl = code_data
    return code_data_tbl


def cal_table(all_code_data):
    all_code_data['price_adj'] = all_code_data['']
    tbl_all_code = all_code_data.groupby(by=['code', 'date']).apply(cal_func)
    return tbl_all_code


def prepare_table():
    start_date = '20200101'
    end_date = '20210101'
    fields = []
    all_code_data = load_data_from_wind_eodprices(start_date, end_date, fields)
    tbl_all_code = cal_table(all_code_data)
    pass


prepare_table()
