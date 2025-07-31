import pandas as pd


def cal_date_range(start_date, end_date):
    date_list = pd.Series(pd.date_range(start_date, end_date, closed='left')).dt.strftime('%Y-%m-%d').to_list()
    return date_list


def cal_tradeday_range(start_date, end_date, tradedays, inclu='both'):
    rtn = pd.Series(tradedays)
    rtn = rtn[rtn.between(start_date, end_date, inclusive=inclu)]
    return rtn


def cal_change_month(time_start, delta_time):
    time_change = (pd.to_datetime(time_start) + pd.DateOffset(months=delta_time)).strftime('%Y-%m-%d')
    return time_change


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


def define_term(term, time=1):
    if time == 1:
        start_date = str(int(term[:4])-1) + '-05-01'
        end_date = term[:4] + '-04-30'
    elif time == 0.5:
        start_date = str(int(term[:4])-1) + '-11-01'
        end_date = term[:4] + '-04-30'
    return start_date, end_date


if __name__ == '__main__':
    cal_change_month('2022-05-12', 3)
