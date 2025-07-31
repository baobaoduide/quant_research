import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = 'FangSong'
plt.rcParams['axes.unicode_minus'] = False
from old_codes.load_data.rawdata_util import load_ashare_daily_data, load_trade_days, load_industry_index_data
from old_codes.calc_tools.time_util import get_change_trade_day, get_change_date_day
from old_codes.load_data.load_factor import load_std_factor_rd_expense


def prepare_bench_index_ret(bench_index, start_date, end_date, trade_days):
    frequency = 'M'
    dates_for_adjust = pd.date_range(start=start_date, end=end_date, freq=frequency)
    dates_for_adjust = pd.Series(dates_for_adjust, name='CalcDate').dt.strftime('%Y-%m-%d')
    tradeday_delta = 1
    next_trade_days = dates_for_adjust.apply(func=lambda x: get_change_trade_day(x, tradeday_delta, trade_days)).rename(
        'CalcDate_trade')
    date_df = pd.concat([dates_for_adjust, next_trade_days], axis=1)
    #
    bench_index.rename(columns={'CalcDate': 'CalcDate_trade'}, inplace=True)
    bench_index_ret = pd.merge(date_df, bench_index, on=['CalcDate_trade'], how='left')
    shift_month_num = -1
    bench_index_ret['ret'] = bench_index_ret['close'].shift(shift_month_num) / bench_index_ret['close'] - 1
    bench_index_ret.dropna(subset=['ret'], inplace=True)
    bench_index_ret.drop(columns=['CalcDate_trade', 'close'], inplace=True)
    bench_index_ret.reset_index(drop=True, inplace=True)
    return bench_index_ret


def prepare_ret(ashare_daily, start_date, end_date, trade_days):
    frequency = 'M'
    dates_for_adjust = pd.date_range(start=start_date, end=end_date, freq=frequency)
    dates_for_adjust = pd.Series(dates_for_adjust, name='CalcDate').dt.strftime('%Y-%m-%d')
    tradeday_delta = 1
    next_trade_days = dates_for_adjust.apply(func=lambda x: get_change_trade_day(x, tradeday_delta, trade_days)).rename('CalcDate_trade')
    date_df = pd.concat([dates_for_adjust, next_trade_days], axis=1)
    #
    ashare_daily.rename(columns={'CalcDate': 'CalcDate_trade'}, inplace=True)
    ashare_ret = pd.merge(date_df, ashare_daily, on=['CalcDate_trade'], how='left')
    ashare_ret.sort_values(by=['Code', 'CalcDate_trade'], inplace=True)
    shift_month_num = -1
    close_next = ashare_ret.groupby(by=['Code'])['close_adj'].shift(shift_month_num)
    ashare_ret['ret'] = close_next / ashare_ret['close_adj'] - 1
    ashare_ret.dropna(subset=['ret'], inplace=True)
    ashare_ret.drop(columns=['CalcDate_trade', 'close_adj'], inplace=True)
    ashare_ret.sort_values(by=['CalcDate', 'Code'], inplace=True)
    ashare_ret.reset_index(drop=True, inplace=True)
    return ashare_ret


def test_ic_ir(factor_df_std, ashare_ret, industry_citic):
    factor_and_return = pd.merge(factor_df_std, ashare_ret, on=['CalcDate', 'Code'])
    ic_df = factor_and_return.groupby(by=['CalcDate'], as_index=False).apply(
        func=lambda x: x['factor_std'].corr(x['ret'], method='pearson')).rename(columns={None: 'ic'})
    rank_ic_df = factor_and_return.groupby(by=['CalcDate'], as_index=False).apply(lambda x: x['factor_std'].corr(x['ret'], method='spearman')).rename(columns={None: 'rank_ic'})
    factor_ic_df = pd.merge(ic_df, rank_ic_df, on='CalcDate')
    #
    ic_mean = factor_ic_df['ic'].mean()
    ir = ic_mean / factor_ic_df['ic'].std()
    rank_ic_mean = factor_ic_df['rank_ic'].mean()
    rank_ic_ir = rank_ic_mean / factor_ic_df['rank_ic'].std()
    factor_perf_df = pd.Series({'ic_mean': ic_mean, 'ir': ir, 'rank_ic_mean': rank_ic_mean, 'rank_ic_ir':  rank_ic_ir}, name='factor_ic_ir')
    #
    calcdate = pd.to_datetime(factor_ic_df['CalcDate'])
    rank_ic_mean_list = rank_ic_mean * np.ones([len(calcdate), ])
    plt.figure()
    plt.grid(axis='y', linestyle='-', zorder=1)
    plt.bar(calcdate, factor_ic_df['rank_ic'], label='RankIC', width=16, color='navy', zorder=2)
    plt.plot(calcdate, rank_ic_mean_list, label='RankIC_Mean', color='red', linestyle='--', zorder=3)
    plt.legend()
    plt.title('Factor Monthly RankIC '+industry_citic)
    path_fig = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\figure\\'+industry_citic+'rank_ic_monthly.png'
    plt.savefig(path_fig)
    plt.close()
    return factor_perf_df


def cal_grouped_excess_ret(factor_df_std, ashare_ret, industry_citic):
    factor_and_return = pd.merge(factor_df_std, ashare_ret, on=['CalcDate', 'Code'])
    factor_and_return['ret_ex'] = factor_and_return.groupby(by=['CalcDate'])['ret'].apply(func=lambda x: x-x.mean())
    quantile_num = 10
    factor_and_return['group'] = factor_and_return.groupby(by=['CalcDate'])['factor_std'].apply(func=lambda x: pd.qcut(x, q=quantile_num, labels=[i for i in range(1, quantile_num+1)]))
    grouped_quantile_ret = factor_and_return.groupby(by=['CalcDate', 'group'], as_index=False)['ret_ex'].mean()
    grouped_quantile_ret_mean = grouped_quantile_ret.groupby(by=['group'], as_index=False)['ret_ex'].mean()
    #
    plt.figure()
    plt.bar(grouped_quantile_ret_mean['group'], grouped_quantile_ret_mean['ret_ex'], label='ExReturn', width=0.3,
            color='navy', zorder=2)
    plt.legend()
    plt.grid(axis='y', linestyle='-', zorder=1)
    plt.title('Grouped Excess Return '+industry_citic)
    path_fig = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\figure\\' + industry_citic + 'grouped_excess_ret.png'
    plt.savefig(path_fig)
    plt.close()
    return grouped_quantile_ret_mean


def draw_nv_curve(factor_df_std, ashare_ret, bench_index_ret, industry_citic):
    factor_and_return = pd.merge(factor_df_std, ashare_ret, on=['CalcDate', 'Code'])
    quantile_num = 5
    factor_and_return['group'] = factor_and_return.groupby(by=['CalcDate'])['factor_std'].apply(
        func=lambda x: pd.qcut(x, q=quantile_num, labels=[i for i in range(1, quantile_num + 1)]))
    grouped_quantile_ret = factor_and_return.groupby(by=['CalcDate', 'group'], as_index=False)['ret'].mean()
    quantile_ret_unstack = grouped_quantile_ret.set_index(keys=['CalcDate', 'group'])
    quantile_ret_unstack = quantile_ret_unstack.unstack(level=['group']).shift(1)
    quantile_ret_unstack.fillna(0, inplace=True)
    bench_index_ret = bench_index_ret.set_index('CalcDate').reindex(quantile_ret_unstack.index)
    bench_index_ret.columns = pd.MultiIndex.from_tuples([('ret', 'bench')], names=quantile_ret_unstack.columns.names)
    quantile_ret_unstack = pd.concat([quantile_ret_unstack, bench_index_ret], axis=1)
    quantile_nv_unstack = quantile_ret_unstack.copy()
    quantile_nv_unstack = (quantile_nv_unstack+1).cumprod(axis=0)
    quantile_nv_unstack.columns.set_levels(['nv'], level=0, inplace=True)
    long_short_curve = quantile_nv_unstack[('nv', 5)] / quantile_nv_unstack[('nv', 1)]
    long_bench_curve = quantile_nv_unstack[('nv', 5)] / quantile_nv_unstack[('nv', 'bench')]
    #
    num_period = (len(quantile_nv_unstack)-1)
    return_total = (quantile_nv_unstack.iloc[-1] / quantile_nv_unstack.iloc[0] - 1).rename('ret_all').reset_index(level=0,drop=True)
    return_annual = ((1 + return_total)**(1/(num_period/12)) - 1).rename('ret_annual')
    return_annual_ex = (return_annual - return_annual['bench']).rename('return_annual_ex')
    min_df = quantile_nv_unstack.iloc[::-1].cummin().iloc[::-1]
    drawdown_df = (quantile_nv_unstack - min_df) / quantile_nv_unstack
    max_drawdown = drawdown_df.max(axis=0).rename('max_drawdown').reset_index(level=0, drop=True)
    nv_increase_df = quantile_ret_unstack.iloc[1:]
    sharp_ratio = ((nv_increase_df.mean(axis=0) - nv_increase_df.mean(axis=0)[-1])/nv_increase_df.std(axis=0)).rename('sharp_ratio').reset_index(level=0, drop=True)
    nv_perform = pd.concat([return_total, return_annual, return_annual_ex, max_drawdown, sharp_ratio], axis=1)
    #
    calcdate = pd.to_datetime(quantile_nv_unstack.index)
    fig, ax = plt.subplots()
    twin = ax.twinx()
    p1 = ax.plot(calcdate, quantile_nv_unstack[('nv', 5)], label='top quantile', color='navy')
    p2 = ax.plot(calcdate, quantile_nv_unstack[('nv', 'bench')], label='bench industry index', color='cyan')
    p3 = ax.plot(calcdate, quantile_nv_unstack[('nv', 1)], label='bottom quantile', color='blue')
    p4 = twin.plot(calcdate, long_short_curve, label='top/bottom', color='orangered')
    p5 = twin.plot(calcdate, long_bench_curve, label='top/bench', color='red')
    ax.set_ylabel('nv')
    twin.set_ylabel('long short ratio')
    ax.set_title('long short net value curve '+industry_citic)
    lns = p1 + p2 + p3 + p4+ p5
    labs = [l.get_label() for l in lns]
    ax.legend(lns, labs, loc=0)
    # ax.grid(axis='y', linestyle='-', zorder=1)
    fig.tight_layout()
    path_fig = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\figure\\' + industry_citic + 'nv_curve.png'
    plt.savefig(path_fig)
    plt.close()
    return nv_perform


def test_factor_performance(start_date, end_date, industry_citic):
    industry = industry_citic
    factor_df_std = load_std_factor_rd_expense(start_date, end_date, industry)
    #
    ashare_date_delta = 60
    end_date = get_change_date_day(end_date, ashare_date_delta)
    read_type = 'csv'
    trade_days = load_trade_days(start_date, end_date, read_type)
    bench_index = load_industry_index_data(start_date, end_date, industry_citic)
    bench_index_ret = prepare_bench_index_ret(bench_index, start_date, end_date, trade_days)
    fields_daily = ['close_adj']
    ashare_daily = load_ashare_daily_data(start_date, end_date, fields_daily)
    ashare_ret = prepare_ret(ashare_daily, start_date, end_date, trade_days)
    #
    factor_perf_df = test_ic_ir(factor_df_std, ashare_ret, industry_citic)
    ex_ret_grouped = cal_grouped_excess_ret(factor_df_std, ashare_ret, industry_citic)
    nv_perform = draw_nv_curve(factor_df_std, ashare_ret, bench_index_ret, industry_citic)
    return factor_perf_df, ex_ret_grouped, nv_perform


start_date = '2011-01-01'
end_date = '2020-12-31'
industrys = ['医药', '机械', '电子', '汽车', '通信', '计算机', '基础化工', '电力设备及新能源', '国防军工', '家电', '轻工制造']
for i in range(len(industrys)):
    industry_citic = industrys[i]
    print(i+1, industry_citic)
    factor_perf_df, ex_ret_grouped, nv_perform = test_factor_performance(start_date, end_date, industry_citic)
    path_factor_perf_df = os.path.join(os.getcwd(), 'ic_ir_'+industry_citic+'.csv')
    factor_perf_df.to_csv(path_factor_perf_df)
    path_ex_ret_grouped = os.path.join(os.getcwd(), 'ex_ret_grouped_' + industry_citic + '.csv')
    ex_ret_grouped.to_csv(path_ex_ret_grouped, index=False)
    path_nv_perform = os.path.join(os.getcwd(), 'result_df_' + industry_citic + '.csv')
    nv_perform.to_csv(path_nv_perform)
