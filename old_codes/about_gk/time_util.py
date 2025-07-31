import calendar
from datetime import datetime, timedelta


def adjust_month(date_str, n_months):
    date = datetime.strptime(date_str, '%Y-%m-%d')
    delta = timedelta(days=30.44 * n_months)  # 平均每月30.44天
    adjusted_date = date + delta
    adjusted_date_str = adjusted_date.strftime('%Y-%m-%d')
    return adjusted_date_str


def get_month_end_dates(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    month_end_dates = []
    while start_date <= end_date:
        next_month = start_date.replace(day=28) + timedelta(days=4)
        month_end_date = next_month - timedelta(days=next_month.day)

        if month_end_date <= end_date:
            month_end_dates.append(month_end_date.strftime('%Y-%m-%d'))

        start_date = month_end_date + timedelta(days=1)
    return month_end_dates


def get_next_month_end(input_date):
    date_obj = datetime.strptime(input_date, '%Y-%m-%d')
    year = date_obj.year
    month = date_obj.month
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1
    next_month_first_day = datetime(next_year, next_month, 1)
    _, days_in_next_month = calendar.monthrange(next_year, next_month)
    next_month_end = next_month_first_day + timedelta(days=days_in_next_month - 1)
    return next_month_end.date().strftime('%Y-%m-%d')


def get_month_end_date(input_date):
    year, month, day = map(int, input_date.split('-'))
    _, last_day = calendar.monthrange(year, month)
    month_end_date = f"{year}-{month:02d}-{last_day:02d}"
    return month_end_date
