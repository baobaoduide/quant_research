import pandas as pd
import numpy as np
from core.data_utils.loader import DataLoader
from core.data_utils.storage import DataStorage
import warnings

warnings.filterwarnings("ignore")


class GKModelDecomposer:
	"""GK模型收益分解器"""
	
	def __init__(self, project="gk_model", start_year=None, end_year=None):
		"""
		初始化收益分解器

		:param project: 项目名称
		:param start_year: 起始年份
		:param end_year: 结束年份（默认当前年份）
		"""
		self.data_loader = DataLoader()
		self.data_storage = DataStorage()
		self.project = project
		self.start_year = start_year if start_year else 2011
		self.end_year = end_year if end_year else pd.Timestamp.now().year
	
	def load_index_data(self):
		"""加载指数基础数据"""
		print("加载指数基础数据...")
		df = self.data_loader.load_l2(
			project=self.project,
			table="index_aggregated_info",
			file_format="parquet"
		)
		
		# 筛选年份范围
		df = df[(df["年份"] >= self.start_year) & (df["年份"] <= self.end_year)]
		print(f"加载完成，共 {len(df)} 条记录，年份范围: {self.start_year}-{self.end_year}")
		return df
	
	def calculate_drivers(self, df):
		"""计算收益驱动成分并返回纯净结果"""
		print("计算收益驱动成分...")
		
		# 确保数据按年份排序
		df = df.sort_values(["index_name", "年份"])
		
		# 计算期初值
		df["总市值_0"] = df.groupby("index_name")["总市值"].shift(1)
		df["净利润_TTM_0"] = df.groupby("index_name")["净利润_TTM"].shift(1)
		df["PE_TTM_0"] = df.groupby("index_name")["PE_TTM"].shift(1)
		
		# 计算基础驱动成分
		df["实际收益"] = np.log((df["总市值"] + df["分红市值"]) / df["总市值_0"])
		df["股息驱动"] = np.log(df["分红市值"] / df["总市值"] + 1)
		df["业绩驱动"] = np.log(df["净利润_TTM"] / df["净利润_TTM_0"])
		
		# 使用PE计算估值驱动
		df["估值驱动"] = np.log(df["PE_TTM"] / df["PE_TTM_0"])
		
		# 应用业绩驱动修正
		self._apply_profit_adjustment(df)
		
		# 只保留需要的列
		clean_df = df[[
			"index_name", "index_code", "年份",
			"总市值", "分红市值", "净利润_TTM", "PE_TTM",
			"实际收益", "股息驱动", "业绩驱动", "估值驱动"
		]].copy()
		
		return clean_df
	
	def _apply_profit_adjustment(self, df):
		"""应用业绩驱动修正逻辑"""
		print("应用业绩驱动修正...")
		
		# 遍历每一行应用修正
		for idx, row in df.iterrows():
			profit = row["净利润_TTM"]
			profit_0 = row["净利润_TTM_0"]
			ret_actual = row["实际收益"]
			div = row["股息驱动"]
			
			# 跳过缺失期初值的年份
			if pd.isna(profit_0):
				continue
			
			if profit < 0 and profit_0 > 0:
				# 当期亏损但上期盈利
				profit_ret_adj = (profit - profit_0) / profit_0
				df.at[idx, "业绩驱动"] = profit_ret_adj
				df.at[idx, "估值驱动"] = ret_actual - div - profit_ret_adj
			
			elif profit > 0 and profit_0 < 0:
				# 当期盈利但上期亏损
				adj_value = (ret_actual - div) / 2
				df.at[idx, "业绩驱动"] = adj_value
				df.at[idx, "估值驱动"] = adj_value
			
			elif profit < 0 and profit_0 < 0:
				# 连续亏损
				profit_ret_adj = np.log(profit_0 / profit)
				df.at[idx, "业绩驱动"] = profit_ret_adj
				df.at[idx, "估值驱动"] = ret_actual - div - profit_ret_adj
	
	def save_results(self, df, result_name="gk_decomposition_result"):
		"""保存纯净的分解结果到L3层"""
		print(f"保存分解结果: {result_name}")
		
		# 保存到L3结果层
		save_path = self.data_storage.save_l3(
			df=df,
			project=self.project,
			result_name=result_name,
			file_format="xlsx"  # 也可以改为"excel"如果需要
		)
		
		print(f"纯净结果已保存至: {save_path}")
		return save_path
	
	def decompose_all(self, result_name="gk_decomposition_result"):
		"""分解所有指数并保存纯净结果"""
		print(f"开始分解所有指数 ({self.start_year}-{self.end_year})")
		df = self.load_index_data()
		clean_df = self.calculate_drivers(df)
		save_path = self.save_results(clean_df, result_name)
		
		# 统计信息
		num_indexes = clean_df["index_name"].nunique()
		years_covered = f"{clean_df['年份'].min()}-{clean_df['年份'].max()}"
		
		print(f"完成! 处理了 {num_indexes} 个指数，覆盖年份 {years_covered}")
		return clean_df, save_path
	
	def decompose_single(self, index_name, result_name=None):
		"""分解单个指数并保存纯净结果"""
		result_name = result_name or f"gk_decomposition_{index_name}"
		print(f"开始分解指数: {index_name} ({self.start_year}-{self.end_year})")
		df = self.load_index_data()
		df = df[df["index_name"] == index_name]
		
		if df.empty:
			print(f"警告: 未找到指数 '{index_name}' 的数据")
			return None, None
		
		clean_df = self.calculate_drivers(df)
		save_path = self.save_results(clean_df, result_name)
		
		# 统计信息
		years_covered = f"{clean_df['年份'].min()}-{clean_df['年份'].max()}"
		print(f"完成! 处理了 {len(clean_df)} 个年份 ({years_covered})")
		return clean_df, save_path


if __name__ == '__main__':
	# 示例用法
	
	# 1. 分解所有指数 (2011-2022) 并保存为单个文件
	decomposer = GKModelDecomposer(start_year=2011, end_year=2022)
	result_df, save_path = decomposer.decompose_all(result_name="gk_decomposition_all")
	
	# 2. 分解单个指数并保存
	# decomposer = GKModelDecomposer(start_year=2015, end_year=2022)
	# result_df, save_path = decomposer.decompose_single("沪深300")
	
	# 3. 分解最近5年数据
	# current_year = pd.Timestamp.now().year
	# decomposer = GKModelDecomposer(start_year=current_year-5)
	# result_df, save_path = decomposer.decompose_all()
	
	# 4. 如果需要Excel格式用于检查