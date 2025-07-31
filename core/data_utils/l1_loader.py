from pathlib import Path
from core.config import L1_PROCESSED_DATA
import logging
import pandas as pd
import re
from typing import Optional

logger = logging.getLogger("l1_loader")


class L1Loader:
	"""精简版 L1 数据加载器 - 基于文件命名的版本管理"""
	
	def __init__(self, cache_enabled=True):
		"""
		:param cache_enabled: 是否启用内存缓存
		"""
		self.cache = {} if cache_enabled else None
	
	def load(
			self,
			category: str,
			data_type: str,
			date: Optional[str] = None,
			start_date: Optional[str] = None,
			end_date: Optional[str] = None,
			date_column: str = 'date'
	) -> pd.DataFrame:
		"""
		智能加载 L1 数据
		- 加载指定或最新版本的文件
		- 对于时间序列数据，按日期范围过滤

		:param category: 数据类别
		:param data_type: 数据类型
		:param date: 数据日期 (YYYYMMDD 或 'latest')
		:param start_date: 开始日期 (YYYYMMDD)，仅时间序列数据有效
		:param end_date: 结束日期 (YYYYMMDD)，仅时间序列数据有效
		:param date_column: 用于过滤的日期列名
		:return: 包含数据的 DataFrame
		"""
		# 1. 获取文件路径
		file_path = self._get_file_path(category, data_type, date)
		
		# 2. 加载数据
		df = self._load_parquet(file_path)
		
		# 3. 按日期过滤（如果需要）
		if start_date or end_date:
			df = self._filter_by_date(df, start_date, end_date, date_column)
		
		return df
	
	def get_latest_date(self, category: str, data_type: str) -> Optional[str]:
		"""获取指定数据类型的最新日期 (YYYYMMDD)"""
		data_dir = L1_PROCESSED_DATA / category / data_type
		if not data_dir.exists():
			return None
		
		# 获取所有文件并提取日期
		dates = []
		for file in data_dir.glob("*.parquet"):
			if date := self._extract_date(file.name):
				dates.append(date)
		
		return max(dates) if dates else None
	
	def _get_file_path(
			self,
			category: str,
			data_type: str,
			date: Optional[str] = None
	) -> Path:
		"""获取数据文件路径"""
		data_dir = L1_PROCESSED_DATA / category / data_type
		
		# 确定要加载的日期
		target_date = date or self.get_latest_date(category, data_type)
		if not target_date:
			raise FileNotFoundError(f"找不到数据: {category}/{data_type}")
		
		# 尝试查找匹配文件
		for file in data_dir.glob("*.parquet"):
			if self._extract_date(file.name) == target_date:
				return file
		
		# 如果没有精确匹配，尝试前缀匹配
		for file in data_dir.glob(f"{target_date}*.parquet"):
			return file
		
		raise FileNotFoundError(f"找不到 {target_date} 的数据文件")
	
	def _load_parquet(self, file_path: Path) -> pd.DataFrame:
		"""加载 Parquet 文件"""
		# 检查缓存
		if self.cache and file_path in self.cache:
			return self.cache[file_path].copy()
		
		# 加载数据
		logger.info(f"加载 L1 数据: {file_path}")
		df = pd.read_parquet(file_path)
		
		# 更新缓存
		if self.cache is not None:
			self.cache[file_path] = df.copy()
		
		return df
	
	def _filter_by_date(
			self,
			df: pd.DataFrame,
			start_date: Optional[str],
			end_date: Optional[str],
			date_column: str
	) -> pd.DataFrame:
		"""按日期范围过滤数据"""
		# 检查日期列是否存在
		if date_column not in df.columns:
			logger.warning(f"日期列 '{date_column}' 不存在，跳过过滤")
			return df
		
		# 转换为日期时间
		try:
			df[date_column] = pd.to_datetime(df[date_column])
		except Exception as e:
			logger.error(f"日期转换失败: {str(e)}")
			return df
		
		# 设置日期范围
		start_dt = pd.to_datetime(start_date) if start_date else None
		end_dt = pd.to_datetime(end_date) if end_date else None
		
		# 创建过滤掩码
		if start_dt and end_dt:
			mask = (df[date_column] >= start_dt) & (df[date_column] <= end_dt)
		elif start_dt:
			mask = df[date_column] >= start_dt
		elif end_dt:
			mask = df[date_column] <= end_dt
		else:
			return df
		
		# 应用过滤
		return df.loc[mask].copy()
	
	def _extract_date(self, filename: str) -> Optional[str]:
		"""从文件名中提取日期部分 (YYYYMMDD)"""
		match = re.search(r"(\d{8})", filename)
		return match.group(1) if match else None


if __name__ == '__main__':
	loader = L1Loader()
	df = loader.load(category='financials',
			data_type='ashare_income',
			date='20230821',)
