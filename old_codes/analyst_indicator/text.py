import re
import pandas as pd
from old_codes.calc_tools.rawdata_util import load_data_from_gogoal


def cal_is_text_ex_expect(analyst_forecast_text):
    pattern = '(超.*预期)|(好于.*预期)|(高于.*预期)|(优于.*预期)'
    func_is_ex_expect = lambda x: int(re.search(pattern, x) != None) if isinstance(x, str) else 0
    is_ex_expect_title = analyst_forecast_text['title'].apply(func=func_is_ex_expect)
    is_ex_expect_content = analyst_forecast_text['content'].apply(func=func_is_ex_expect)
    analyst_forecast_ex_expect = analyst_forecast_text[['Code', 'create_date']]
    analyst_forecast_ex_expect.rename(columns={'create_date': 'CalcDate'}, inplace=True)
    analyst_forecast_ex_expect['is_ex_expect_title'] = is_ex_expect_title
    analyst_forecast_ex_expect['is_ex_expect_content'] = is_ex_expect_content
    analyst_forecast_ex_expect = analyst_forecast_ex_expect.groupby(by=['Code', 'CalcDate'], as_index=False).apply(lambda x: pd.Series({'is_ex_expect_title': int(x['is_ex_expect_title'].sum() > 0), 'is_ex_expect_content': int(x['is_ex_expect_content'].sum() > 0)}))
    return analyst_forecast_ex_expect


def calc_features(start_cal_date, end_cal_date):
    read_type = 'csv'
    fields_analyst = ['content']
    analyst_forecast_text = load_data_from_gogoal(start_cal_date, end_cal_date, fields_analyst, read_type)
    analyst_forecast_ex_expect = cal_is_text_ex_expect(analyst_forecast_text)
    return analyst_forecast_ex_expect


# start_cal_date = '2020-01-01'
# end_cal_date = '2020-12-31'
# calc_features(start_cal_date, end_cal_date)
