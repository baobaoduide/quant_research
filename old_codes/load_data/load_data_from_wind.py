import pandas as pd
import pymysql


""" 底层数据库连接 """
host = "192.168.41.56"
port = 3306
user = "inforesdep01"
passwd = "tfyfInfo@1602"
db = "wind"
conn = pymysql.connect(user=user, host=host, port=port, passwd=passwd, db=db)


sql = "select *" \
      "from AShareSWIndustriesClass"
AShareSWIndustriesClass = pd.read_sql(sql, con=conn)
path_AShareANNFinancialIndicator = r'E:\NJU_term4\TF_Intern\公司创新能力刻画因子\数据\AShareANNFinancialIndicator.csv'
AShareANNFinancialIndicator.to_csv(path_AShareANNFinancialIndicator, index=False)


# rd_expense_copy = rd_expenditure.copy()
# rd_expense_copy['report_quarter'] = rd_expense_copy['REPORT_PERIOD'].apply(func=lambda x: int(int(x[4:6])/3))
# rd_expense_copy = rd_expense_copy[rd_expense_copy['report_quarter'] == 4]
# temp = rd_expense_copy['ANN_DT'].drop_duplicates().sort_values()
# temp2 = rd_expense_copy['REPORT_PERIOD'].drop_duplicates().sort_values()


temp = AShareANNFinancialIndicator[['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD', 'RD_EXPENSE']]
temp.sort_values(by=['S_INFO_WINDCODE', 'REPORT_PERIOD', 'ANN_DT'], inplace=True)
temp2 = temp.dropna()
temp2.sort_values(by=['ANN_DT'], inplace=True)


gogoal_sql = {'host': '192.168.41.62', 'port': 3306, 'user': 'cusreader', 'passwd': 'cus_reader@tf1603',
                'db': 'cus_fund_db2'}
gogoal_conn = pymysql.connect(charset='utf8', **gogoal_sql)
rtn = pd.read_sql("select * from rpt_forecast_stk ", con=gogoal_conn)
