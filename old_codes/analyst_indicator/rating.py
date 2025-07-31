from old_codes.calc_tools.rawdata_util import load_data_from_gogoal


def calc_features(start_cal_date, end_cal_date):
    read_type = 'csv'
    fields_analyst = ['organ_rating_content']
    analyst_forecast_np = load_data_from_gogoal(start_cal_date, end_cal_date, fields_analyst, read_type)
    pass


import pandas as pd


start_cal_date = '2010-01-01'
end_cal_date = '2020-12-31'
# calc_features(start_cal_date, end_cal_date)
path_data = r'E:\NJU_term4\TF_Intern\ReplicateReport\Data\DataOrigin\rpt_forecast_stk\rpt_forecast_stk'
rtn = pd.DataFrame()
for year in range(int(start_cal_date[:4]), int(end_cal_date[:4])+1):
    forecast_data_i = pd.read_csv(path_data+"_"+str(year)+".csv", dtype={'stock_code': str})
    rtn = rtn.append(forecast_data_i)
rtn.rename(columns={'stock_code': 'Code'}, inplace=True)
rtn = rtn[rtn["create_date"].between(start_cal_date, end_cal_date, inclusive="both")]
gg_rating_content = rtn['gg_rating_content'].unique()
rtn.sort_values(by=['organ_name', 'organ_rating_content'], inplace=True)
organ_rating_content = rtn.groupby(by='organ_name')['organ_rating_content'].unique()
organ_rating_all = rtn['organ_rating_content'].drop_duplicates().sort_values()
temp = rtn[rtn['gg_rating_content'] == '无']

# path_rating_content = r'E:\NJU_term4\TF_Intern\EngineerWork\analyst_related_record\organ_rating_content.csv'
# organ_rating_content.to_csv(path_rating_content, encoding="utf_8_sig")

# organ_rating_gg = rtn.groupby(by='gg_rating_content')['organ_rating_content'].unique()
# path_organ_rating_gg = r'E:\NJU_term4\TF_Intern\EngineerWork\analyst_related_record\organ_rating_gg.csv'
# organ_rating_gg.to_csv(path_rating_content, encoding="utf_8_sig")

# temp = rtn[rtn['organ_name'] == '']

# path_gg_rating_ = r'E:\NJU_term4\TF_Intern\EngineerWork\analyst_related_record\评级问题\rpt_gogoal_rating.csv'
# gg_rating_ = pd.read_csv(path_gg_rating_)

security_data = rtn[rtn['organ_name'] == '信达证券']
security_data.sort_values(by=['create_date', 'title', 'report_year'], inplace=True)
organ_rating_security = security_data[security_data['organ_rating_content'] == '无']
