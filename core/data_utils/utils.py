import pandas as pd


def format_date_str(date_series):
	"""
	将YYYYMMDD格式字符串转换为YYYY-MM-DD格式
	支持Series和单个字符串输入
	"""
	if isinstance(date_series, pd.Series):
		# 向量化操作提高效率
		return date_series.str[:4] + '-' + date_series.str[4:6] + '-' + date_series.str[6:8]
	
	if isinstance(date_series, str) and len(date_series) == 8:
		return f"{date_series[:4]}-{date_series[4:6]}-{date_series[6:8]}"
	
	return date_series  # 非字符串或长度不符则原样返回
