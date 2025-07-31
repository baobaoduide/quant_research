from pathlib import Path
import pandas as pd
import logging
from .cache import DataCache
from typing import Optional


class BaseDataLoader:
	def __init__(self, cache_enabled: bool = True):
		self.cache = DataCache() if cache_enabled else None
		self.logger = logging.getLogger("data_loader")
	
	def load_data(self, path: Path, cache_key: Optional[str] = None, **kwargs) -> pd.DataFrame:
		"""加载数据（支持 CSV、Excel、Parquet）"""
		if not path.exists():
			raise FileNotFoundError(f"路径不存在: {path}")
		
		# 缓存检查
		if self.cache and cache_key:
			if cached := self.cache.get(cache_key):
				self.logger.debug(f"使用缓存数据: {cache_key}")
				return cached
		
		# 根据文件后缀加载
		suffix = path.suffix.lower()
		
		try:
			if suffix in ('.xlsx', '.xls'):
				data = pd.read_excel(path, **kwargs)
			elif suffix == '.parquet':
				data = pd.read_parquet(path, **kwargs)
			elif suffix == '.csv':
				# 为 CSV 添加特殊处理
				data = self._load_csv_with_encoding_detection(path, **kwargs)
			else:
				raise ValueError(f"不支持的格式: {suffix}")
		except Exception as e:
			self.logger.error(f"加载失败: {path} | 错误: {str(e)}")
			raise
		
		# 更新缓存
		if self.cache and cache_key:
			self.cache.set(cache_key, data)
		
		return data
	
	def _load_csv_with_encoding_detection(self, path: Path, **kwargs) -> pd.DataFrame:
		"""自动检测 CSV 文件编码"""
		encodings = ['utf-8', 'gbk', 'gb18030', 'latin1', 'iso-8859-1']
		
		for encoding in encodings:
			try:
				return pd.read_csv(path, encoding=encoding, **kwargs)
			except UnicodeDecodeError:
				continue
			except Exception as e:
				self.logger.warning(f"尝试编码 {encoding} 失败: {str(e)}")
		
		# 所有编码尝试失败
		raise UnicodeDecodeError(f"无法解析CSV编码: {path}")