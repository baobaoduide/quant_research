from pathlib import Path
from core.data_utils import l0_loader
from core.data_utils.utils import format_date_str
from core.config import l1_storage
import logging
import pandas as pd

# 配置日志
logger = logging.getLogger("preprocessing.corporate_actions")


class CorporateActionsPreprocessor:
	"""数据预处理类"""
	
	def __init__(self, date: str = None):
		"""
		:param date: 指定处理哪天的数据（YYYYMMDD），默认处理最新数据
		"""
		self.date = date
		self.processed_data = {}
	
	def _load_raw_data(self, filename: str, dtype: dict = None) -> pd.DataFrame:
		"""加载原始数据文件"""
		try:
			if self.date:
				df = l0_loader.load_raw_file(
					source='wind',
					category='corporate_actions',
					date=self.date,
					filename=filename,
					dtype=dtype
				)
				logger.info(f"加载指定日期数据: {self.date} - {filename}")
			else:
				df = l0_loader.load_latest_file(
					source='wind',
					category='corporate_actions',
					filename=filename,
					dtype=dtype
				)
				logger.info(f"加载最新数据 - {filename}")
			return df
		except Exception as e:
			logger.error(f"数据加载失败: {filename} - {str(e)}", exc_info=True)
			raise
	
	def preprocess_ashare_dividend(self) -> Path:
		"""预处理数据"""
		try:
			# 1. 加载原始数据
			df = self._load_raw_data(
				'中国A股分红[AShareDividend].csv',
				dtype={'REPORT_PERIOD': str, 'EX_DT': str}
			)
			logger.info(f"开始预处理，原始数据形状: {df.shape}")
			
			# 2. 数据清洗与转换
			name_dict = {'S_INFO_WINDCODE': 'code',
			             'EX_DT': 'date',
			             'REPORT_PERIOD': 'report_period',
			             'CASH_DVD_PER_SH_PRE_TAX': 'cash_div_ps_pre',
			             'CASH_DVD_PER_SH_AFTER_TAX': 'cash_div_ps_aft',
			             'S_DIV_BASESHARE': 'base_share'}
			df = df[name_dict.keys()]
			df.rename(columns=name_dict, inplace=True)
			
			# 处理日期
			df.dropna(subset=['date'], inplace=True)
			df['date'] = format_date_str(df['date'])
			
			df['base_share'] = df['base_share'] * 10000
			
			# 排序和重置索引
			df.sort_values(by=['code', 'date'], inplace=True)
			df.reset_index(drop=True, inplace=True)
			
			# 3. 保存处理结果
			save_path, metadata = l1_storage.save_processed_data(
				df=df,
				category="corporate_actions",
				table_name="ashare_dividend",
				date=self.date,
				metadata={'source': 'wind'}
			)
			return save_path, metadata
		
		except Exception as e:
			logger.error(f"预处理失败: {str(e)}", exc_info=True)
			raise
	
	
	def preprocess_all(self):
		"""预处理数据"""
		results = {}
		results['ashare_dividend'] = self.preprocess_ashare_dividend()
		# 添加更多表...
		return results
	
	def get_processing_summary(self):
		"""获取处理摘要"""
		summary = {
			'date': self.date or "latest",
			'tables_processed': list(self.processed_data.keys()),
			'details': self.processed_data
		}
		return summary


# 使用示例
if __name__ == "__main__":
	processor = CorporateActionsPreprocessor(date="20230821")
	processor.preprocess_all()
	
	summary = processor.get_processing_summary()
	print("处理摘要:")
	for table, info in summary['details'].items():
		print(f"- {table}: {info['rows']}行, 保存至 {info['path']}")
		
	
	