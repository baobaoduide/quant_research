from core.data_utils.loader import DataLoader
from core.data_utils.storage import DataStorage
import pandas as pd


def load_gk_base_data():
	"""加载GK模型所需基础数据"""
	DATA_LOADER = DataLoader()
	
	date = '20230821'
	
	# 加载财务报表数据
	income = DATA_LOADER.load_l1(dataset='financials', date=date, table='ashare_net_profit_ttm_annual')
	
	# 加载行情数据
	prices = DATA_LOADER.load_l1(dataset='market', date=date, table='ashare_eod_derivativeindicator')
	
	# 加载分红数据
	ashare_dividend = DATA_LOADER.load_l1(dataset='corporate_actions', date=date, table='ashare_dividend')
	
	return {
		'income': income,
		'prices': prices,
		'dividend': ashare_dividend
	}


def prepare_base_data():
	raw_data = load_gk_base_data()
	
	data_mv = raw_data['prices']
	data_mv.rename(columns={'market_value': '总市值'}, inplace=True)
	data_mv['年份'] = data_mv['date'].str[:4].astype(int)
	data_mv.sort_values(by=['code', 'date'], inplace=True)
	data_mv_year = data_mv.groupby(by=['code', '年份'], as_index=False)['总市值'].last()
	
	data_div = raw_data['dividend']
	data_div['分红市值'] = data_div['cash_div_ps_pre'] * data_div['base_share']
	data_div['年份'] = data_div['date'].apply(func=lambda x: int(x[:4]))
	data_div_year = data_div.groupby(by=['code', '年份'], as_index=False)['分红市值'].sum()
	
	income = raw_data['income']
	income.rename(columns={'year': '年份', 'profit_ttm': '净利润_TTM'}, inplace=True)
	#
	year_data = pd.merge(data_mv_year, data_div_year, on=['code', '年份'], how='outer')
	year_data = pd.merge(year_data, income, on=['code', '年份'], how='outer')
	year_data = year_data[year_data['年份'] > 2008]
	year_data.drop(columns=['report_period'], inplace=True)
	
	DATA_STORAGE = DataStorage()
	save_path = DATA_STORAGE.save_l2(
		df=year_data,
		project="gk_model",
		table="ashare_base_info",
		file_format='parquet')
	return year_data, save_path


if __name__ == '__main__':
	df, path = prepare_base_data()
