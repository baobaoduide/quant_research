import os
import pandas as pd
import pymysql


def load_AIndexValuation():
    wind_sql = {'host': '192.168.41.56', 'port': 3306, 'user': 'inforesdep01', 'passwd': 'tfyfInfo@1602',
                'db': 'wind'}
    conn = pymysql.connect(charset='utf8', **wind_sql)
    rtn = pd.read_sql("select S_INFO_WINDCODE,TRADE_DT,EST_YOYPROFIT_Y1,EST_YOYPROFIT_Y2 "
                          "from aindexvaluation", con=conn)
    return rtn


folder = ''
aindexvaluation = load_AIndexValuation()
path_data = os.path.join(folder, 'aindexvaluation.csv')
aindexvaluation.to_csv(path_data, index=False)
