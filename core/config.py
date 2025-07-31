from pathlib import Path
import os


class Paths:
	"""独立路径配置类，不依赖其他模块"""
	
	def __init__(self):
		self.DATA_WAREHOUSE = Path(os.getenv("DATA_WAREHOUSE", "D:/code_workspace/data_warehouse"))
		
		# 基础路径
		self.L0_PATH = self.DATA_WAREHOUSE / "L0_raw_data"
		self.L1_PATH = self.DATA_WAREHOUSE / "L1_processed_data"
		
		# 确保路径存在
		self.L0_PATH.mkdir(parents=True, exist_ok=True)
		self.L1_PATH.mkdir(parents=True, exist_ok=True)
	
	def get_l2_data_path(self, project: str) -> Path:
		"""获取项目L2数据路径"""
		path = self.DATA_WAREHOUSE / "projects" / project / "data"
		path.mkdir(parents=True, exist_ok=True)
		return path
	
	def get_l3_results_path(self, project: str) -> Path:
		"""获取项目L3结果路径"""
		path = self.DATA_WAREHOUSE / "projects" / project / "results"
		path.mkdir(parents=True, exist_ok=True)
		return path
