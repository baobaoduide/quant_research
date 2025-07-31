import logging
import pandas as pd
from pathlib import Path
from core.config import Paths

logger = logging.getLogger("data.storage")


class DataStorage:
	"""统一数据存储工具，精简接口设计"""
	
	def __init__(self):
		self.paths = Paths()
	
	def save_l1(
			self,
			df: pd.DataFrame,
			dataset: str,
			table: str,
			date: str,
			file_format: str = "parquet"
	) -> Path:
		"""
		保存L1处理后的数据

		:param df: 要保存的DataFrame
		:param dataset: 数据集 (financials/market)
		:param table: 表名
		:param date: 处理日期 (YYYYMMDD)
		:param file_format: 文件格式
		:return: 保存的文件路径
		"""
		save_dir = self.paths.L1_PATH / dataset / table
		save_dir.mkdir(parents=True, exist_ok=True)
		save_path = save_dir / f"{table}_{date}.{file_format}"
		self._save_file(df, save_path, file_format)
		return save_path
	
	def save_l2(
			self,
			df: pd.DataFrame,
			project: str,
			table: str,
			file_format: str = "parquet"
	) -> Path:
		"""
		保存项目L2数据 (平铺结构)

		:param df: 要保存的DataFrame
		:param project: 项目名称
		:param table: 表名 (可包含子目录)
		:param file_format: 文件格式
		:return: 保存的文件路径
		"""
		base_path = self.paths.get_l2_data_path(project)
		save_path = base_path / f"{table}.{file_format}"
		save_path.parent.mkdir(parents=True, exist_ok=True)
		self._save_file(df, save_path, file_format)
		return save_path
	
	def save_l3(
			self,
			df: pd.DataFrame,
			project: str,
			result_name: str,
			file_format: str = "parquet"
	) -> Path:
		"""
		保存项目L3结果 (平铺结构)

		:param df: 要保存的DataFrame
		:param project: 项目名称
		:param result_name: 结果名称 (可包含子目录)
		:param file_format: 文件格式
		:return: 保存的文件路径
		"""
		base_path = self.paths.get_l3_results_path(project)
		save_path = base_path / f"{result_name}.{file_format}"
		save_path.parent.mkdir(parents=True, exist_ok=True)
		self._save_file(df, save_path, file_format)
		return save_path
	
	def _save_file(
			self,
			df: pd.DataFrame,
			save_path: Path,
			file_format: str
	):
		"""统一文件保存方法"""
		try:
			if file_format == "csv":
				df.to_csv(save_path, index=False)
			elif file_format == "parquet":
				df.to_parquet(save_path)
			elif file_format == "feather":
				df.to_feather(save_path)
			elif file_format in ("xlsx", "xls"):
				df.to_excel(save_path)
			elif file_format == "pkl":
				df.to_pickle(save_path)
			else:
				raise ValueError(f"不支持的文件格式: {file_format}")
			logger.info(f"数据保存成功: {save_path}")
		except Exception as e:
			logger.error(f"数据保存失败: {save_path} - {e}")
			raise
		