import os
import pandas as pd


def cal_cover_ratio(factor_df):
    factor_df_stack = factor_df.set_index(keys=['Code', 'CalcDate']).stack(level=-1)
    factor_cover_df = factor_df_stack.reset_index(name='factor_pt').rename(columns={'level_2': 'factor_type'})
    factor_cover_df['is_factor'] = (factor_cover_df['factor_pt'] > 0).astype(int)
    cover_ratio_df = factor_cover_df.groupby(by=['factor_type', 'CalcDate'])['is_factor'].agg(cover_ratio=lambda x: x.sum()/len(x))
    cover_ratio_df = cover_ratio_df.unstack(level=['factor_type'])
    #
    cover_ratio_df.index = pd.DatetimeIndex(cover_ratio_df.index)
    cover_ratio_df = cover_ratio_df.resample('Y').asfreq()
    cover_ratio_df.index = cover_ratio_df.index.strftime('%Y-%m-%d')
    cover_ratio_df_sorted = cover_ratio_df.T.sort_values(by=['2020-12-31'], ascending=False)
    return cover_ratio_df


def cal_corr(factor_df_date):
    corr_df_date = factor_df_date.iloc[:, 2:]
    corr_df_date = corr_df_date.corr()
    return corr_df_date


def cal_factor_corr(factor_df):
    factor_corr = factor_df.groupby(by='CalcDate').apply(cal_corr)
    end_factor_corr = factor_corr.loc['2020-12-31']
    factor_corr_mean = factor_corr['2011-01-01':]
    # factor_corr_mean.reset_index(level=-1, inplace=True)
    factor_corr_mean = factor_corr_mean.groupby(level=1).mean()
    factor_corr_mean = factor_corr_mean.reindex(factor_corr_mean.columns)
    return factor_corr, end_factor_corr, factor_corr_mean


def test_pt_factor_cover(start_date, end_date):
    path_pt_factor = r'E:\NJU_term4\TF_Intern\project_comp_inno_growth\result_data\factor_pt_num_inc.csv'
    factor_df = pd.read_csv(path_pt_factor)
    factor_df = factor_df[(factor_df['CalcDate'] >= start_date) & (factor_df['CalcDate'] <= end_date)]
    #
    factor_cover_ratio = cal_cover_ratio(factor_df)
    factor_corr, end_factor_corr, factor_corr_mean = cal_factor_corr(factor_df)
    return factor_cover_ratio, factor_corr, end_factor_corr, factor_corr_mean


start_date = '2004-01-01'
end_date = '2020-12-31'
factor_cover_df, factor_corr, end_factor_corr, factor_corr_mean = test_pt_factor_cover(start_date, end_date)
path_factor_corr = os.path.join(os.getcwd(), 'pt_factor_corr.csv')
factor_corr.to_csv(path_factor_corr)
path_end_factor_corr = os.path.join(os.getcwd(), 'pt_end_factor_corr.csv')
end_factor_corr.to_csv(path_end_factor_corr)
path_factor_corr_mean = os.path.join(os.getcwd(), 'pt_factor_corr_mean.csv')
factor_corr_mean.to_csv(path_factor_corr_mean)
