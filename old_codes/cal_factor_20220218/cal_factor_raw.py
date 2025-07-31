import os
import pandas as pd
from load_data_from_local import load_eod_price_data, load_asharecashflow, load_asharedescription, load_AshareFinancialderivative, load_AShareEODDerivativeIndicator, load_asharedividend, load_eod_price_data, load_fcf
folder = r'H:\TF_Intern\data_20220218\raw_factor'


def cal_factor_exist_asfd(AshareFinancialderivative, asharedescription):
    factor_df = pd.merge(asharedescription, AshareFinancialderivative, on='comp_id')
    name_dict = {'ROE': 'ROE', 'YOYEQUITY': 'YOYAssets', 'YOYASSETS': 'YOYTotAsset', 'YOYNETPROFIT': 'YOYProfit', 'YOYNETPROFIT_DEDUCTED': 'YOYProfitDuct', 'YOY_OR': 'YOYSales', 'OPTOGR': 'GrossMargin', 'NETPROFITMARGIN': 'NetMargin', 'ROIC': 'ROIC'}
    factor_df['DebtToAsset'] = factor_df['ASSETSTOEQUITY'] - 1
    factor_df.rename(columns=name_dict, inplace=True)
    factor_df = factor_df[(factor_df['date'] >= '2009-12-31') & (factor_df['date'] <= '2021-12-31')]
    factor_df.drop(columns=['comp_id', 'fiscal_year', 'FCFF', 'ASSETSTOEQUITY'], inplace=True)
    path_data = os.path.join(folder, 'raw_factor_10.csv')
    factor_df.to_csv(path_data, index=False)
    return factor_df


def cal_factor_exist_aseoddi(AShareEODDerivativeIndicator, asharecashflow, asharedividend):
    factor_5e = AShareEODDerivativeIndicator.copy()
    factor_5e['BP'] = 1 / factor_5e['S_VAL_PB_NEW']
    factor_5e['EPTTM'] = 1 / factor_5e['S_VAL_PE_TTM']
    name_dict = {'S_VAL_PCF_OCFTTM': 'PCFO', 'S_VAL_PS_TTM': 'PSTTM', 'S_VAL_MV': 'TotalMV'}
    factor_5e.rename(columns=name_dict, inplace=True)
    factor_5e.drop(columns=['S_VAL_PB_NEW', 'S_VAL_PE_TTM'], inplace=True)
    factor_5e['date_ye'] = factor_5e['date'].apply(func=lambda x: x[:4]+'-12-31')
    factor_5e_year = factor_5e.groupby(by=['StockCode', 'date_ye'], as_index=False).last()
    factor_5e_year.drop(columns=['date'], inplace=True)
    factor_5e_year.rename(columns={'date_ye': 'date'}, inplace=True)
    factor_5e_year = factor_5e_year[(factor_5e_year['date'] >= '2009-12-31') & (factor_5e_year['date'] <= '2021-12-31')]
    path_data = os.path.join(folder, 'raw_factor_5.csv')
    factor_5e_year.to_csv(path_data, index=False)
    #
    free_cash_flow = asharecashflow.copy()
    free_cash_flow.rename(columns={'year': 'date'}, inplace=True)
    fcfp_factor = pd.merge(free_cash_flow, factor_5e_year[['StockCode', 'date', 'TotalMV']], on=['StockCode', 'date'])
    fcfp_factor['FCFP'] = fcfp_factor['free_cash_flow'] / fcfp_factor['TotalMV']
    fcfp_factor.drop(columns=['free_cash_flow', 'TotalMV'], inplace=True)
    path_data = os.path.join(folder, 'raw_factor_fcfp.csv')
    fcfp_factor.to_csv(path_data, index=False)
    #
    div_data = asharedividend.copy()
    div_data['div'] = div_data['cash_div_ps_pre'] * div_data['base_share']
    div_data['date'] = div_data['report_period'].apply(func=lambda x: x[:4]+'-12-31')
    div_data_new = div_data.groupby(by=['StockCode', 'date'], as_index=False)['div'].sum()
    div_factor = pd.merge(div_data_new, factor_5e_year[['StockCode', 'date', 'TotalMV']], on=['StockCode', 'date'])
    div_factor['DP'] = div_factor['div'] / div_factor['TotalMV']
    div_factor.drop(columns=['div', 'TotalMV'], inplace=True)
    path_data = os.path.join(folder, 'raw_factor_dp.csv')
    div_factor.to_csv(path_data, index=False)
    return AShareEODDerivativeIndicator


def cal_Momentum(eod_prices):
    eod_prices['close'] = eod_prices['close'] * eod_prices['adj_factor']
    eod_prices['year_m'] = eod_prices['date'].apply(func=lambda x: x[:7])
    eod_prices['year'] = eod_prices['date'].apply(func=lambda x: x[:4]+'-12-31')
    #
    eod_prices_month = eod_prices.groupby(by=['StockCode', 'year_m'], as_index=False)['close'].last()
    eod_prices_month['close_lastm'] = eod_prices_month.groupby(by='StockCode')['close'].shift(1)
    eod_prices_month['ret_m'] = eod_prices_month['close'] / eod_prices_month['close_lastm'] - 1
    eod_prices_month['year'] = eod_prices_month['year_m'].apply(func=lambda x: x[:4]+'-12-31')
    month_ret_df = eod_prices_month.groupby(by=['StockCode', 'year'], as_index=False)['ret_m'].last()
    #
    eod_prices_year = eod_prices.groupby(by=['StockCode', 'year'], as_index=False)['close'].last()
    eod_prices_year['close_lasty'] = eod_prices_year.groupby(by=['StockCode'])['close'].shift(1)
    eod_prices_year['ret_y'] = eod_prices_year['close'] / eod_prices_year['close_lasty'] - 1
    #
    momentum_df = pd.merge(eod_prices_year[['StockCode', 'year', 'ret_y']], month_ret_df, on=['StockCode', 'year'])
    momentum_df['Momentum'] = momentum_df['ret_m']*12 - momentum_df['ret_y']
    momentum_df.rename(columns={'year': 'date'}, inplace=True)
    momentum_df.dropna(inplace=True)
    momentum_df.drop(columns=['ret_y', 'ret_m'], inplace=True)
    momentum_df = momentum_df[(momentum_df['date'] >= '2009-12-31') & (momentum_df['date'] <= '2021-12-31')]
    path_data = os.path.join(folder, 'raw_factor_Momentum.csv')
    momentum_df.to_csv(path_data, index=False)
    return eod_prices


# def cal_YOYProfitFY(AshareFinancialderivative, asharedescription):
#     YOYProfitFY_df = pd.merge(asharedescription, AshareFinancialderivative[['comp_id', 'date', 'YOYNETPROFIT']], on='comp_id')
#     YOYProfitFY_df['YOYProfitFY1'] = YOYProfitFY_df.groupby(by='StockCode')['YOYNETPROFIT'].shift(-1)
#     YOYProfitFY_df['YOYNETPROFIT_f2'] = YOYProfitFY_df.groupby(by='StockCode')['YOYNETPROFIT'].shift(-2)
#     YOYProfitFY_df['YOYProfitFY2'] = (YOYProfitFY_df['YOYProfitFY1']+1) * (YOYProfitFY_df['YOYProfitFY1']+1) - 1
#     YOYProfitFY_df.drop(columns=['comp_id', 'YOYNETPROFIT', 'YOYNETPROFIT_f2'], inplace=True)
#     #
#     YOYProfitFY_df = YOYProfitFY_df[(YOYProfitFY_df['date'] >= '2010-12-31') & (YOYProfitFY_df['date'] <= '2021-12-31')]
#     YOYProfitFY_df.dropna(subset=['YOYProfitFY1', 'YOYProfitFY2'], how='all', inplace=True)
#     path_data = os.path.join(folder, 'raw_factor_YOYProfitFY.csv')
#     YOYProfitFY_df.to_csv(path_data, index=False)
#     pass


if __name__ == '__main__':
    # AshareFinancialderivative = load_AshareFinancialderivative()
    # asharedescription = load_asharedescription()
    # cal_factor_exist_asfd(AshareFinancialderivative, asharedescription)
    #
    AShareEODDerivativeIndicator = load_AShareEODDerivativeIndicator()
    asharecashflow = load_fcf()
    asharedividend = load_asharedividend()
    cal_factor_exist_aseoddi(AShareEODDerivativeIndicator, asharecashflow, asharedividend)
    #
    eod_prices = load_eod_price_data()
    cal_Momentum(eod_prices)
    #
