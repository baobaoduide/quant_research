import os
import pandas as pd


def load_factors(factor_name):
    folder = r'H:\TF_Intern\data_20220218\raw_factor'
    raw_factor_10 = ['ROE', 'YOYAssets', 'YOYTotAsset', 'YOYProfit', 'YOYProfitDuct', 'YOYSales', 'GrossMargin', 'NetMargin', 'ROIC', 'DebtToAsset']
    raw_factor_5 = ['PCFO', 'PSTTM', 'TotalMV', 'BP', 'EPTTM']
    raw_factor_YOYProfitFY = ['YOYProfitFY1', 'YOYProfitFY2']
    if factor_name in raw_factor_10:
        path_data = os.path.join(folder, 'raw_factor_10.csv')
        raw_factor = pd.read_csv(path_data)
    elif factor_name in raw_factor_5:
        path_data = os.path.join(folder, 'raw_factor_5.csv')
        raw_factor = pd.read_csv(path_data)
    elif factor_name == 'DP':
        path_data = os.path.join(folder, 'raw_factor_dp.csv')
        raw_factor = pd.read_csv(path_data)
    elif factor_name == 'FCFP':
        path_data = os.path.join(folder, 'raw_factor_fcfp.csv')
        raw_factor = pd.read_csv(path_data)
    elif factor_name == 'Momentum':
        path_data = os.path.join(folder, 'raw_factor_Momentum.csv')
        raw_factor = pd.read_csv(path_data)
    elif factor_name in raw_factor_YOYProfitFY:
        path_data = os.path.join(folder, 'raw_factor_YOYProfitFY.csv')
        raw_factor = pd.read_csv(path_data)
    raw_factor_one = raw_factor[['StockCode', 'date', factor_name]]
    num_dup = raw_factor.duplicated(subset=['StockCode', 'date']).sum()
    raw_factor_one.drop_duplicates(subset=['StockCode', 'date'], inplace=True)
    return raw_factor_one, num_dup


def removeoutlier_and_normalize(factor):
    mean = factor.mean()
    std = factor.std()
    upper_bound = mean + 3*std
    lower_bound = mean - 3*std
    factor = factor.mask(factor > upper_bound, upper_bound)
    factor = factor.mask(factor < lower_bound, lower_bound)
    #
    mean = factor.mean()
    std = factor.std()
    normalized_factor = (factor - mean) / std
    return normalized_factor


def norm_factor2():
    factor_names = ['ROE', 'YOYAssets', 'YOYTotAsset', 'YOYProfit', 'YOYProfitDuct', 'YOYSales', 'GrossMargin',
                    'NetMargin', 'ROIC', 'DebtToAsset', 'PCFO', 'PSTTM', 'TotalMV', 'BP', 'EPTTM', 'FCFP',
                    'Momentum']
    factor_df_all = []
    for factor_name in factor_names:
        factor_df, num_dup = load_factors(factor_name)
        factor_df_norm = factor_df.copy().set_index(['StockCode', 'date'])
        factor_df_norm = factor_df_norm.unstack('StockCode')
        factor_df_all.append(factor_df_norm)
    factor_all = pd.concat(factor_df_all, axis=1, join='outer')
    factor_all.fillna(method='ffill', inplace=True)
    factor_all_stack = factor_all.stack(level='StockCode')
    factor_all_stack = factor_all_stack.groupby(level='date').apply(func=lambda x: x.fillna(x.mean()))
    dp_data = load_factors('DP')[0].set_index(['date', 'StockCode'])
    factor_all_dp = pd.merge(factor_all_stack, dp_data, left_index=True, right_index=True, how='left')
    factor_all_dp['DP'].fillna(0, inplace=True)
    #
    factor_names.append('DP')
    for factor_name in factor_names:
        factor_df = factor_all_dp[factor_name]
        factor_df_norm = factor_df.groupby(level=['date']).apply(removeoutlier_and_normalize)
        factor_all_dp[factor_name] = factor_df_norm
    factor_all_dp = factor_all_dp.T
    for date in factor_all_dp.columns.levels[0]:
        factor_year = factor_all_dp[date].T
        folder = r'H:\TF_Intern\data_20220218\factor_yearly'
        path_data = os.path.join(folder, date + '.csv')
        factor_year.to_csv(path_data)
    return factor_all_dp


def norm_factor(factor_df, factor_name):
    factor_df_norm = factor_df.copy().rename(columns={factor_name: 'raw_factor'})
    factor_df_norm[factor_name] = factor_df_norm.groupby(by=['date'])['raw_factor'].apply(removeoutlier_and_normalize)
    factor_df_norm.drop(columns='raw_factor', inplace=True)
    factor_df_norm.set_index(['StockCode', 'date'], inplace=True)
    return factor_df_norm


if __name__ == '__main__':
    norm_factor2()
    factor_names = ['ROE', 'YOYAssets', 'YOYTotAsset', 'YOYProfit', 'YOYProfitDuct', 'YOYSales', 'GrossMargin', 'NetMargin', 'ROIC', 'DebtToAsset', 'PCFO', 'PSTTM', 'TotalMV', 'BP', 'EPTTM', 'DP', 'FCFP', 'Momentum']
    factor_df_all = []
    num_dups = []
    for factor_name in factor_names:
        factor_df, num_dup = load_factors(factor_name)
        factor_std = norm_factor(factor_df, factor_name)
        factor_df_all.append(factor_std)
        num_dups.append(num_dup)
    check_df = pd.DataFrame({'fn': factor_names, 'dunum': num_dups})
    factor_all = pd.concat(factor_df_all, axis=1, join='outer')
    factor_all_unstack = factor_all.unstack('date')
    factor_all_unstack = factor_all_unstack.swaplevel(i=- 2, j=- 1, axis=1)
    check_num = factor_all_unstack.count()
    for date in factor_all_unstack.columns.levels[0]:
        factor_year = factor_all_unstack[date]
        factor_year.dropna(how='all', inplace=True)
        folder = r'H:\TF_Intern\data_20220218\factor_yearly'
        path_data = os.path.join(folder, date+'.csv')
        factor_year.to_csv(path_data)
    print('end')