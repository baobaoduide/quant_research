import pandas as pd


def get_change_date_day(date_, day_delta):
    time_change = pd.Timestamp(date_) + pd.Timedelta(days=day_delta)
    time_change = time_change.strftime('%Y-%m-%d')
    return time_change


def cal_date_range(start_cal_date, end_cal_date):
    date_range = pd.Series(pd.date_range(start_cal_date, end_cal_date))
    date_range = date_range.dt.strftime('%Y-%m-%d')
    return date_range


def cal_trade_date_range(start_cal_date, end_cal_date, trade_days):
    date_range = cal_date_range(start_cal_date, end_cal_date)
    trade_date_range = date_range[date_range.isin(trade_days)]
    return trade_date_range


def get_change_trade_day(date, trade_day_delta, trade_days):
    if date in trade_days:
        index_new = trade_days.index(date) + trade_day_delta
        if index_new <= len(trade_days)-1:
            trade_day_change = trade_days[index_new]
        else:
            trade_day_change = None
    else:
        num_list = [int(x < date) for x in trade_days]
        if trade_day_delta > 0:
            index_new = sum(num_list) + trade_day_delta-1
            if index_new <= len(trade_days)-1:
                trade_day_change = trade_days[index_new]
            else:
                trade_day_change = None
        elif trade_day_delta < 0:
            index_new = sum(num_list) + trade_day_delta
            if index_new >= 0:
                trade_day_change = trade_days[index_new]
            else:
                trade_day_change = None
        else:
            trade_day_change = date
    return trade_day_change
