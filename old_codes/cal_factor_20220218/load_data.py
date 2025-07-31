import os
import pandas as pd
import pymysql


def load_ashareeodprices(start_date, end_date):
    wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602',
                'db': 'wind'}
    conn = pymysql.connect(charset='utf8', **wind_sql)
    rtn = pd.read_sql("select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE,S_DQ_ADJFACTOR "
                          "from ashareeodprices where TRADE_DT between {0} and {1}".format(
                           start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
    return rtn


def load_asharedividend(start_date, end_date):
    wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602',
                'db': 'wind'}
    conn = pymysql.connect(charset='utf8', **wind_sql)
    rtn = pd.read_sql("select S_INFO_WINDCODE,REPORT_PERIOD,CASH_DVD_PER_SH_PRE_TAX,CASH_DVD_PER_SH_AFTER_TAX,S_DIV_BASESHARE "
                          "from asharedividend".format(
                           start_date.replace("-", ""), end_date.replace("-", "")), con=conn)
    return rtn


def load_asharedescription():
    wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602',
                'db': 'wind'}
    conn = pymysql.connect(charset='utf8', **wind_sql)
    rtn = pd.read_sql("select S_INFO_WINDCODE,S_INFO_COMPCODE "
                      "from asharedescription", con=conn)
    return rtn


folder = ''
asharedividend = load_asharedividend('2010-01-01', '2022-01-01')
path_data2 = os.path.join(folder, 'asharedividend.csv')
asharedividend.to_csv(path_data2, index=False)

ashareeodprices = load_ashareeodprices('2021-01-01', '2022-02-18')
path_data3 = os.path.join(folder, 'ashareeodprices.csv')
ashareeodprices.to_csv(path_data3, index=False)
