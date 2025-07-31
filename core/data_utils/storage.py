import logging
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Optional

logger = logging.getLogger("data.storage")


class DataStorage:
	"""统一数据存储工具类"""
	
	def __init__(self, base_path: Path):
		"""
		:param base_path: L1处理数据的根目录
		"""
		self.base_path = base_path
	
	def save_processed_data(
			self,
			df: pd.DataFrame,
			category: str,
			table_name: str,
			date: Optional[str] = None,
			metadata: Optional[Dict[str, Any]] = None
	) -> Path:
		"""
		保存处理后的数据

		:param df: 要保存的DataFrame
		:param category: 数据类别（如financials, market等）
		:param table_name: 表名
		:param date: 处理日期（YYYYMMDD格式），可选
		:param metadata: 额外的元数据信息，可选
		:return: 保存的文件路径
		"""
		save_dir = self.base_path / category / table_name
		save_dir.mkdir(parents=True, exist_ok=True)
		
		processed_date = date if date else "latest"
		save_path = save_dir / f"{table_name}_{processed_date}.parquet"
		
		# 保存数据
		df.to_parquet(save_path)
		logger.info(f"{category}/{table_name} 数据保存完成！路径: {save_path}")
		
		# 构建元数据
		save_metadata = {
			'path': save_path,
			'rows': len(df),
			'columns': list(df.columns),
			'category': category,
			'table_name': table_name,
			'processed_date': processed_date
		}
		
		if metadata:
			save_metadata.update(metadata)
		
		return save_path, save_metadata