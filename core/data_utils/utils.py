from pathlib import Path
import pandas as pd


def detect_file_format(path: Path) -> str:
	"""检测文件格式"""
	suffix = path.suffix.lower()
	
	if suffix == ".parquet":
		return "parquet"
	elif suffix == ".csv":
		return "csv"
	elif suffix in (".h5", ".hdf5", ".hdf"):
		return "hdf"
	elif suffix == ".feather":
		return "feather"
	else:
		raise ValueError(f"无法识别的文件格式: {suffix}")


def merge_datasets(loader, datasets: list) -> pd.DataFrame:
	"""合并多个数据集"""
	combined = pd.DataFrame()
	for dataset in datasets:
		source, data_type, date, filename = dataset
		df = loader.load_source_data(source, data_type, date, filename)
		combined = pd.concat([combined, df], ignore_index=True)
	return combined


def read_excel_file(path: Path, **kwargs) -> pd.DataFrame:
	"""读取 Excel 文件"""
	try:
		return pd.read_excel(path, **kwargs)
	except Exception as e:
		raise ValueError(f"读取 Excel 失败: {path} | 错误: {str(e)}")


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
