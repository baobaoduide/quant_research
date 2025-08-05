import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams['font.family'] = 'SimHei'
from WindPy import w
import warnings

warnings.filterwarnings('ignore')
folder_raw = r'D:\code_workspace\data_warehouse\L0_raw_data\wind\cb_section'
name_dict_cb = {'wind_code': '证券代码', 'sec_name': '证券简称', 'CLOSE': '收盘价', 'YTM_CB': '纯债到期收益率',
                'CONVVALUE': '转换价值',
                'CONVPREMIUMRATIO': '转股溢价率', 'STRBPREMIUMRATIO': '纯债溢价率', 'OUTSTANDINGBALANCE': '债券余额',
                'IPO_DATE': '上市日期', 'STRBVALUE': '纯债价值', 'UNDERLYINGCODE': '正股代码'}
name_dict_stock = {'INDUSTRY_SW_2021': '所属申万一级行业', 'MKT_CAP_ARD': '总市值'}


def prepare_cb_section_data(date):
	""""针对某一天，下载全部转债的截面数据"""
	date_num = date.replace('-', '')
	params = f"date={date};sectorid=a101020600000000"
	cb_section = w.wset("sectorconstituent", params, usedf=True)[1][['wind_code', 'sec_name']]
	cb_section['后缀'] = cb_section['wind_code'].apply(func=lambda x: x.split('.')[1])
	cb_section_use = cb_section[cb_section['后缀'].isin(['SH', 'SZ'])]
	cb_section_use.drop(columns=['后缀'], inplace=True)
	cb_section_use.reset_index(drop=True, inplace=True)
	
	cb_list = cb_section_use['wind_code'].to_list()
	cb_param = ','.join(cb_list)
	cb_f_list = list(name_dict_cb.keys())
	cb_fields = [x.lower() for x in cb_f_list[2:]]
	cb_fields_use = ','.join(cb_fields)
	result_df = w.wss(cb_param, cb_fields_use, f"tradeDate={date};priceAdj=U;cycle=D", usedf=True)[1]
	# 提取指标包括收盘价、纯债到期收益率、转换价值(平价)、转股溢价率、纯债溢价率、正股代码
	result_df.reset_index(inplace=True)
	result_df.rename(columns={'index': 'wind_code'}, inplace=True)
	#
	cb_section_use_ = pd.merge(cb_section_use, result_df, on='wind_code', how='left')
	print('数据拉取结果：\n', cb_section_use_.count())
	
	underlying_code = result_df['UNDERLYINGCODE'].drop_duplicates().to_list()
	cb_stock_param = ','.join(underlying_code)
	stock_f_list = list(name_dict_stock.keys())
	stock_fields = [x.lower() for x in stock_f_list]
	stock_fields_use = ','.join(stock_fields)
	industry_df = w.wss(cb_stock_param, stock_fields_use, f"tradeDate={date_num};industryType=1", usedf=True)[1]
	industry_df.reset_index(inplace=True)
	industry_df.rename(columns={'index': 'UNDERLYINGCODE'}, inplace=True)
	
	cb_section_use_ = pd.merge(cb_section_use_, industry_df, on='UNDERLYINGCODE', how='left')
	
	cb_section_use_.rename(columns=name_dict_cb, inplace=True)
	cb_section_use_.rename(columns=name_dict_stock, inplace=True)
	#
	# 文件保存
	path_save = os.path.join(folder_raw, 'cb_section_' + date_num + '.xlsx')
	cb_section_use_.to_excel(path_save, index=False)
	return cb_section_use_


def add_certain_cb_fields_wind(start_d='20250101'):
	"""针对已有日度的截面数据，回溯新添加从万得下载的指标"""
	pattern = r'cb_section_*.xlsx'
	file_list = glob.glob(os.path.join(folder_raw, pattern))
	for file in file_list:
		date = file[64:72]
		if date >= start_d:
			print(date)
			# 校验数据部分
			df = pd.read_excel(file)
			cb_f_add_list = [key.lower() for key, value in name_dict_cb.items() if value not in df.columns]
			stock_f_add_list = [key.lower() for key, value in name_dict_stock.items() if value not in df.columns]
			if len(cb_f_add_list) > 0:
				cb_list = df['证券代码'].to_list()
				cb_param = ','.join(cb_list)
				fields_ = ','.join(cb_f_add_list)
				result_df_cb = \
					w.wss(cb_param, fields_,
					      f"tradeDate={date};priceAdj=U;cycle=D", usedf=True)[1]
				result_df_cb.reset_index(inplace=True)
				result_df_cb.rename(columns={'index': '证券代码'}, inplace=True)
				result_df_cb.rename(columns=name_dict_cb, inplace=True)
				#
				df = pd.merge(df, result_df_cb, on='证券代码', how='left')
			if len(stock_f_add_list) > 0:
				stock_list = df['正股代码'].drop_duplicates().to_list()
				stock_param = ','.join(stock_list)
				fields_ = ','.join(stock_f_add_list)
				result_df_s = \
					w.wss(stock_param, fields_, f"unit=1;tradeDate={date}", usedf=True)[1]
				result_df_s.reset_index(inplace=True)
				result_df_s.rename(columns={'index': '正股代码'}, inplace=True)
				result_df_s.rename(columns=name_dict_stock, inplace=True)
				#
				df = pd.merge(df, result_df_s, on='正股代码', how='left')
			column_names = list(name_dict_cb.values()) + list(name_dict_stock.values())
			df = df[column_names]
			df.to_excel(file, index=False)
			print('转债字段添加：', len(cb_f_add_list), '；股票字段添加：', len(stock_f_add_list), '；总字段数：',
			      len(name_dict_cb) + len(name_dict_stock))
	pass


if __name__ == '__main__':
	w.start()
	w.isconnected()
	date = "2025-08-05"
	result = prepare_cb_section_data(date)
	w.stop()
