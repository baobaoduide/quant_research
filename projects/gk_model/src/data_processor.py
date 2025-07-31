from core.data_utils.l1_loader import L1Loader
import pandas as pd


def load_gk_base_data():
	"""加载GK模型所需基础数据"""
	loader = L1Loader()
	
	# 加载财务报表数据
	income = loader.load('financials', 'ashare_net_profit_ttm_annual', date='20230821')
	
	# 加载行情数据
	prices = loader.load(
		'market', 'ashare_eod_derivativeindicator', date='20230821', start_date='20090101'
	)
	
	# 加载公司描述数据
	companies = loader.load('security_master', 'ashare_description', date='20230821')
	
	# 加载分红数据
	ashare_dividend = loader.load('corporate_actions', 'ashare_dividend', date='20230821')
	
	return {
		'income': income,
		'prices': prices,
		'companies': companies,
		'dividend': ashare_dividend
	}


def prepare_base_data():
	raw_data = load_gk_base_data()
	
	base_info = raw_data['companies']
	
	data_mv = raw_data['prices']
	data_mv.rename(columns={'market_values': '总市值'}, inplace=True)
	data_mv['年份'] = data_mv['date'].apply(func=lambda x: int(x[:4]))
	data_mv.sort_values(by=['code', 'date'], inplace=True)
	data_mv_year = data_mv.groupby(by=['code', '年份'], as_index=False)['总市值'].last()
	
	data_div = raw_data['dividend']
	data_div['分红市值'] = data_div['cash_div_ps_pre'] * data_div['base_share']
	data_div['年份'] = data_div['date'].apply(func=lambda x: int(x[:4]))
	data_div_year = data_div.groupby(by=['code', '年份'], as_index=False)['分红市值'].sum()
	
	income = raw_data['income']
	income = income.stack().reset_index().rename(columns={'level_0': 'code', 'level_1': '年份', 0: '净利润_TTM'})
	income['年份'] = income['年份'].astype(int)
	#
	year_data = pd.merge(data_mv_year, data_div_year, on=['code', '年份'], how='outer')
	year_data = pd.merge(year_data, income, on=['code', '年份'], how='outer')
	path_save = os.path.join(folder, 'all_stock_basic_info.csv')
	year_data.to_csv(path_save, index=False, encoding='gbk')
	pass


if __name__ == '__main__':
	df = prepare_base_data()
	
	load_gk_base_data()

