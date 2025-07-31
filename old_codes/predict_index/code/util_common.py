import math
import datetime
import pandas as pd
from util_time import cal_date_range


def check_index(index_member, term):
    today = datetime.date.today().strftime('%Y-%m-%d')
    index_member['date_out'].fillna(today, inplace=True)
    index_member['date'] = [cal_date_range(s, e) for s, e in
                            zip(index_member['date_in'],
                                index_member['date_out'])]
    index_member = index_member.explode('date').drop(columns=['date_in', 'date_out'])
    index_member.sort_values(by=['date', 'code'], inplace=True)
    member_last = index_member[index_member['date'] == term[:5]+'05-01']['code']
    return member_last


def prepare_mv_amount(share_total, money_data, tradedays):
    mv_amount = pd.merge(share_total, money_data, on=['code', 'date'], how='outer')
    mv_amount.sort_values(by=['code', 'date'], inplace=True)
    mv_amount['share_total'].fillna(method='ffill', inplace=True)
    mv_amount = mv_amount[mv_amount['date'].isin(tradedays)]
    #
    mv_amount['mv'] = mv_amount['share_total'] * mv_amount['close']
    mv_amount.dropna(subset=['close'], inplace=True)
    return mv_amount


def adj_ffshare(share_per):
    if share_per <= 15:
        share_per_new = math.ceil(share_per)
    elif share_per <= 80:
        share_per_new = math.ceil(share_per/10)*10
    else:
        share_per_new = 100
    return share_per_new


def prepare_mv_amount2(share_total, share_ff, money_data, tradedays):
    share = pd.merge(share_total, share_ff, on=['code', 'date'], how='outer')
    mv_amount = pd.merge(share, money_data, on=['code', 'date'], how='outer')
    mv_amount.sort_values(by=['code', 'date'], inplace=True)
    mv_amount['share_total'].fillna(method='ffill', inplace=True)
    mv_amount['free_shares'].fillna(method='ffill', inplace=True)
    mv_amount = mv_amount[mv_amount['date'].isin(tradedays)]
    #
    mv_amount['freeshare_per'] = mv_amount['free_shares'] / mv_amount['share_total'] * 100
    mv_amount['freeshare_per_adj'] = mv_amount['freeshare_per'].apply(adj_ffshare)
    mv_amount['freeshare_adj'] = mv_amount['freeshare_per_adj'] / 100 * mv_amount['share_total']
    #
    mv_amount['mv'] = mv_amount['share_total'] * mv_amount['close']
    mv_amount['mv_f_adj'] = mv_amount['freeshare_adj'] * mv_amount['close']
    mv_amount = mv_amount[['code', 'date', 'amount', 'mv', 'mv_f_adj', 'is_trade']]
    mv_amount.dropna(subset=['mv'], inplace=True)
    return mv_amount


if __name__ == '__main__':
    check_index([], [])
