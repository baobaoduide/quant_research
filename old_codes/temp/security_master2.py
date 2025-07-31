from pathlib import Path
from core.data_utils import l0_loader
from core.data_utils.utils import format_date_str
from core.config import L1_PROCESSED_DATA
import logging
import chardet  # 用于高级编码检测

# 配置日志
logger = logging.getLogger("preprocessing.security_master")


def detect_csv_encoding(file_path: Path) -> str:
	"""检测 CSV 文件的编码"""
	with open(file_path, 'rb') as f:
		rawdata = f.read(10000)  # 读取前10KB用于检测
		result = chardet.detect(rawdata)
		return result['encoding']


def preprocess_ashare_description(date: str = None) -> Path:
	"""
	预处理A股基本资料数据（L0 → L1）

	:param date: 指定处理哪天的数据（YYYYMMDD），默认处理最新数据
	:return: 处理后的文件路径
	"""
	try:
		# 1. 加载原始数据
		if date:
			df = l0_loader.load_raw_file(
				source='wind',
				category='security_master',
				date=date,
				filename='中国A股基本资料[AShareDescription].csv',
				dtype={'S_INFO_LISTDATE': str, 'S_INFO_DELISTDATE': str}
			)
			logger.info(f"加载指定日期数据: {date}")
		else:
			df = l0_loader.load_latest_file(
				source='wind',
				category='security_master',
				filename='中国A股基本资料[AShareDescription].csv',
				dtype={'S_INFO_LISTDATE': str, 'S_INFO_DELISTDATE': str}
			)
			logger.info("加载最新数据")
		
		# 2. 数据清洗与转换
		logger.info(f"开始数据预处理，原始数据形状: {df.shape}")
		
		name_dict = {'S_INFO_WINDCODE': 'code', 'S_INFO_NAME': 'name', 'S_INFO_LISTDATE': 'date_in',
		             'S_INFO_DELISTDATE': 'date_out', 'S_INFO_LISTBOARDNAME': 'list_board'}
		df = df[name_dict.keys()]
		df.rename(columns=name_dict, inplace=True)
		#
		df.dropna(subset=['date_in'], inplace=True)
		df['date_in'] = format_date_str(df['date_in'])
		df['date_out'] = format_date_str(df['date_out'])
		df.sort_values(by=['code', 'date_in'], inplace=True)
		df.reset_index(drop=True, inplace=True)

		
		# 3. 保存到L1
		save_dir = L1_PROCESSED_DATA / "security_master"
		save_dir.mkdir(parents=True, exist_ok=True)
		
		# 生成文件名（带日期）
		processed_date = date if date else "latest"
		save_path = save_dir / f"ashare_description_{processed_date}.parquet"
		
		df.to_parquet(save_path)
		logger.info(f"预处理完成！保存至: {save_path}")
		
		return save_path
	
	except Exception as e:
		logger.error(f"预处理失败: {str(e)}", exc_info=True)
		raise


if __name__ == '__main__':
	preprocess_ashare_description('20230821')
	# ashare_eod = l0_loader.load_raw_file(
	# 	source='wind',
	# 	category='market',
	# 	date='20230821',
	# 	filename='中国A股日行情估值指标[AShareEODDerivativeIndicator].csv'
	# )
