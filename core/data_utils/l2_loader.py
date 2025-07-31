from pathlib import Path
import pandas as pd
from .base_loader import BaseDataLoader
from core.config import get_project_dir, PathConfig
from typing import Optional


class L2Loader(BaseDataLoader):
	"""L2项目数据加载器"""
	
	def load_project_data(
			self,
			project_name: str,
			data_file: str,
			cache_key: Optional[str] = None,
			**kwargs
	) -> pd.DataFrame:
		"""
		加载项目数据

		:param project_name: 项目名称 (e.g., 'cb_strategy')
		:param data_file: 数据文件路径 (相对路径)
		:param cache_key: 自定义缓存键
		"""
		project_path = get_project_dir(project_name)
		path = project_path / "data" / data_file
		
		# 自动生成缓存键
		if cache_key is None:
			cache_key = f"l2_{project_name}_{data_file}"
		
		return self.load_data(path, cache_key=cache_key, **kwargs)
	
	def load_project_config(
			self,
			project_name: str,
			config_file: str = "config.yaml"
	) -> dict:
		"""加载项目配置文件"""
		project_path = get_project_dir(project_name)
		config_path = project_path / config_file
		
		# 这里简化处理，实际可使用PyYAML等库
		if config_path.suffix == ".json":
			import json
			with open(config_path, "r") as f:
				return json.load(f)
		else:
			raise ValueError("不支持的配置文件格式")