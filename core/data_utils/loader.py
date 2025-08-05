import logging
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from config import Paths  # 仅导入路径配置

logger = logging.getLogger("data.loader")


class DataLoader:
	"""独立数据加载器"""
	
	def __init__(self):
		self.paths = Paths()
	
	def load_l0(
			self,
			source: str,
			dataset: str,
			filename: str,
			date: Optional[str] = None,
			file_format: str = "csv",
			dtype: Optional[Dict[str, Any]] = None
	) -> pd.DataFrame:
		# 构建文件路径
		if date:
			file_path = self.paths.L0_PATH / source / dataset / date / f"{filename}.{file_format}"
		else:
			date_dirs = [d for d in (self.paths.L0_PATH / source / dataset).iterdir() if d.is_dir()]
			if not date_dirs:
				raise FileNotFoundError(f"未找到数据: {source}/{dataset}")
			latest_date = sorted(date_dirs, key=lambda x: x.name, reverse=True)[0]
			file_path = latest_date / f"{filename}.{file_format}"
		
		return self._load_file(file_path, file_format, dtype)
	
	def load_l1(
			self,
			dataset: str,
			table: str,
			date: Optional[str] = None,
			file_format: str = "parquet",
			dtype: Optional[Dict[str, Any]] = None
	) -> pd.DataFrame:
		table_dir = self.paths.L1_PATH / dataset / table
		if date:
			file_path = table_dir / f"{table}_{date}.{file_format}"
		else:
			files = list(table_dir.glob(f"*.{file_format}"))
			if not files:
				raise FileNotFoundError(f"未找到数据: {dataset}/{table}")
			file_path = max(files, key=lambda f: f.stat().st_mtime)
		
		return self._load_file(file_path, file_format, dtype)
	
	def load_l2(
			self,
			project: str,
			table: str,
			file_format: str = "parquet",
			dtype: Optional[Dict[str, Any]] = None
	) -> pd.DataFrame:
		base_path = self.paths.get_l2_data_path(project)
		file_path = base_path / f"{table}.{file_format}"
		return self._load_file(file_path, file_format, dtype)
	
	def load_l3(
			self,
			project: str,
			result_name: str,
			file_format: str = "parquet",
			dtype: Optional[Dict[str, Any]] = None
	) -> pd.DataFrame:
		base_path = self.paths.get_l3_results_path(project)
		file_path = base_path / f"{result_name}.{file_format}"
		return self._load_file(file_path, file_format, dtype)
	
	def _load_file(
			self,
			file_path: Path,
			file_format: str,
			dtype: Optional[Dict[str, Any]] = None
	) -> pd.DataFrame:
		if not file_path.exists():
			raise FileNotFoundError(f"文件不存在: {file_path}")
		
		try:
			if file_format == "csv":
				return pd.read_csv(file_path, dtype=dtype, low_memory=False)
			elif file_format == "parquet":
				return pd.read_parquet(file_path)
			elif file_format == "feather":
				return pd.read_feather(file_path)
			elif file_format in ("xlsx", "xls"):
				return pd.read_excel(file_path, dtype=dtype)
			elif file_format in ("pkl", "pickle"):
				return pd.read_pickle(file_path)
			else:
				raise ValueError(f"不支持的文件格式: {file_format}")
		except Exception as e:
			logger.error(f"文件加载失败: {file_path} - {e}")
			raise


if __name__ == '__main__':
	loader = DataLoader()
	df = loader.load_l0(source='wind', dataset='index_data', filename='中国A股指数成份股[AIndexMembers]')
	
