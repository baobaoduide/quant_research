import os
import pandas as pd
import pymysql


def load_asharedividend():
    wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602',
                'db': 'wind'}
    conn = pymysql.connect(charset='utf8', **wind_sql)
    rtn = pd.read_sql("select S_INFO_WINDCODE,EX_DT,REPORT_PERIOD,CASH_DVD_PER_SH_PRE_TAX,CASH_DVD_PER_SH_AFTER_TAX,S_DIV_BASESHARE "
                          "from asharedividend", con=conn)
    return rtn


folder = ''
asharedividend = load_asharedividend()
path_data2 = os.path.join(folder, 'asharedividend.csv')
asharedividend.to_csv(path_data2, index=False)
