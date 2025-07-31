from con_forecast import calc_features as calc_con_forecast_features
from forecast_change_ann_related import calc_features as calc_forecast_change_ann_features
from text import calc_features as calc_text_features
from compare_forecast_ann_ex_notice_increase import calc_features as calc_forecast_ann_ex_notice_features
from compare_forecast_ann_ueperc import calc_features as calc_forecast_ann_ueperc_features


def calc_indicator(root_path_, category_, scd, ecd):
    if category_ == 'con_forecast':
        df = calc_con_forecast_features(scd, ecd)
    elif category_ == 'forecast_change_ann':
        df = calc_forecast_change_ann_features(scd, ecd)
    elif category_ == 'text':
        df = calc_text_features(scd, ecd)
    elif category_ == 'forecast_ann_ex_notice':
        df = calc_forecast_ann_ex_notice_features(scd, ecd)
    elif category_ == 'forecast_ann_ueperc':
        df = calc_forecast_ann_ueperc_features(scd, ecd)
    else:
        assert False, "analyst_indicator>>generator>>unknown category:{0}.".format(category_)
    return df


root_path = r''
category = 'con_forecast'
start_cal_date = '2020-01-01'
end_cal_date = '2020-12-31'
calc_indicator(root_path, category, start_cal_date, end_cal_date)
