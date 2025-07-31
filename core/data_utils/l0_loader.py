from .base_loader import BaseDataLoader
from core.config import L0_RAW_DATA
import logging
import pandas as pd

logger = logging.getLogger("l0_loader")


class L0Loader(BaseDataLoader):
	"""支持日期目录的 L0 数据加载器"""
	
	def load_raw_file(
			self,
			source: str,
			category: str,
			date: str,
			filename: str,
			**kwargs
	) -> pd.DataFrame:
		"""
		加载 L0 原始文件（支持 CSV、Excel 等）

		:param source: 数据源 (e.g., 'wind')
		:param category: 数据类别 (e.g., 'security_master')
		:param date: 数据日期 (YYYYMMDD 格式)
		:param filename: 文件名 (e.g., '中国A股基本资料[AShareDescription].csv')
		"""
		# 构建路径
		path = L0_RAW_DATA / source / category / date / filename
		
		# 检查路径是否存在
		if not path.exists():
			raise FileNotFoundError(f"文件不存在: {path}")
		
		logger.info(f"加载原始文件: {path}")
		return self.load_data(path, **kwargs)
	
	def list_available_dates(
			self,
			source: str,
			category: str
	) -> list:
		"""列出指定数据源和类别的所有可用日期"""
		dir_path = L0_RAW_DATA / source / category
		if not dir_path.exists():
			return []
		
		# 获取所有日期子目录
		dates = [d.name for d in dir_path.iterdir() if d.is_dir() and d.name.isdigit()]
		return sorted(dates)
	
	def load_latest_file(
			self,
			source: str,
			category: str,
			filename: str,
			**kwargs
	) -> pd.DataFrame:
		"""加载指定文件的最新版本"""
		dates = self.list_available_dates(source, category)
		if not dates:
			raise FileNotFoundError(f"找不到数据: {source}/{category}")
		
		latest_date = dates[-1]
		return self.load_raw_file(source, category, latest_date, filename, **kwargs)