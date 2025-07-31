from pathlib import Path
from core.data_utils.utils import format_date_str
from core.data_utils.loader import DataLoader
from core.data_utils.storage import DataStorage
import logging
import pandas as pd
from tqdm import tqdm

# 配置日志
logger = logging.getLogger("preprocessing.financials")


class FinancialsPreprocessor:
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
			DATA_LOADER = DataLoader()
			df = DATA_LOADER.load_l0(source='wind', dataset='financials', date=self.date, filename=filename,
			                         dtype=dtype)
			return df
		except Exception as e:
			logger.error(f"数据加载失败: {filename} - {str(e)}", exc_info=True)
			raise
	
	def preprocess_ashare_income(self) -> Path:
		"""预处理数据"""
		try:
			# 1. 加载原始数据
			df = self._load_raw_data(
				'中国A股利润表[AShareIncome]',
				dtype={'ACTUAL_ANN_DT': str, 'REPORT_PERIOD': str}
			)
			logger.info(f"开始预处理，原始数据形状: {df.shape}")
			
			# 2. 数据清洗与转换
			name_dict = {'S_INFO_WINDCODE': 'code',
			             'ACTUAL_ANN_DT': 'date',
			             'REPORT_PERIOD': 'report_period',
			             'STATEMENT_TYPE': 'statement_type',
			             'NET_PROFIT_EXCL_MIN_INT_INC': 'profit'}
			df = df[name_dict.keys()]
			df.rename(columns=name_dict, inplace=True)
			
			df = df[df['statement_type'].isin([408001000, 408005000, 408027000, 408028000])]
			
			# 处理日期
			df.dropna(subset=['date'], inplace=True)
			df['date'] = format_date_str(df['date'])
			
			# 排序和重置索引
			df.sort_values(by=['code', 'date'], inplace=True)
			df.reset_index(drop=True, inplace=True)
			
			# 3. 保存处理结果
			DATA_STORAGE = DataStorage()
			save_path = DATA_STORAGE.save_l1(
				df=df,
				dataset="financials",
				table="ashare_income",
				date=self.date)
			return save_path
		
		except Exception as e:
			logger.error(f"预处理失败: {str(e)}", exc_info=True)
			raise
	
	def preprocess_ashare_balancesheet(self) -> Path:
		"""预处理数据"""
		try:
			# 1. 加载原始数据
			df = self._load_raw_data(
				'asharebalancesheet',
				dtype={'report_period': str, 'actual_ann_dt': str}
			)
			logger.info(f"开始预处理，原始数据形状: {df.shape}")
			
			# 2. 数据清洗与转换
			name_dict = {'S_INFO_WINDCODE': 'code',
			             'ACTUAL_ANN_DT': 'date',
			             'REPORT_PERIOD': 'report_period',
			             'STATEMENT_TYPE': 'statement_type',
			             'TOT_SHRHLDR_EQY_EXCL_MIN_INT': 'net_asset',
			             'TOT_LIAB': 'tot_liab',
			             'TOT_ASSETS': 'tot_assets'}
			name_dict_new = {key.lower(): value for key, value in name_dict.items()}
			df = df[name_dict_new.keys()]
			df.rename(columns=name_dict_new, inplace=True)
			
			df = df[df['statement_type'].isin([408001000, 408005000, 408027000, 408028000])]
			
			# 处理日期
			df.dropna(subset=['date'], inplace=True)
			df['date'] = format_date_str(df['date'])
			
			# 排序和重置索引
			df.sort_values(by=['code', 'date'], inplace=True)
			df.reset_index(drop=True, inplace=True)
			
			# 3. 保存处理结果
			DATA_STORAGE = DataStorage()
			save_path = DATA_STORAGE.save_l1(
				df=df,
				dataset="financials",
				table="ashare_balancesheet",
				date=self.date)
			return save_path
		
		except Exception as e:
			logger.error(f"预处理失败: {str(e)}", exc_info=True)
			raise
	
	def calculate_net_profit_ttm(self) -> Path:
		"""计算净利润TTM（年度视角）"""
		try:
			logger.info("开始计算净利润TTM（年度视角）...")
			
			# 1. 加载预处理后的利润表数据
			DATA_LOADER = DataLoader()
			df_income = DATA_LOADER.load_l1(dataset='financials', date=self.date, table='ashare_income')
			
			# 2. 准备数据
			df_income['report_year'] = df_income['report_period'].str[:4]
			df_income['report_year'] = df_income['report_year'].astype(int)
			df_income['ann_year'] = df_income['date'].str[:4]
			df_income['ann_year'] = df_income['ann_year'].astype(int)
			
			# 3. 确定年份范围（动态获取）
			min_year = df_income['report_year'].min()
			max_year = df_income['report_year'].max()
			all_years = list(range(int(min_year), int(max_year) + 1))
			logger.info(f"计算年份范围: {min_year}-{max_year}")
			
			# 4. 优化计算逻辑（避免双重循环）
			ttm_results = []
			
			# 按股票分组
			grouped = df_income.groupby('code')
			
			for code, group in tqdm(grouped, desc="计算TTM进度", total=len(grouped)):
				group = group.sort_values('ann_year')
				profit_list = []
				
				# 按年处理
				for year in all_years:
					data_use = group[group['ann_year'] <= year]
					
					# 关键日期标识
					suppose_report = f"{year}1231"
					latest_report = f"{year}0930"
					last_end = f"{year - 1}1231"
					last_latest = f"{year - 1}0930"
					
					profit_ttm = None
					
					try:
						# 情况1：存在年报数据
						if not data_use[data_use['report_period'] == suppose_report].empty:
							profit_ttm = data_use.loc[
								data_use['report_period'] == suppose_report, 'profit'
							].values[0]
						
						# 情况2：使用三季报估算
						elif {latest_report, last_end, last_latest}.issubset(data_use['report_period'].values):
							profit_last = data_use.loc[
								data_use['report_period'] == latest_report, 'profit'
							].values[0]
							profit_last_y = data_use.loc[
								data_use['report_period'] == last_end, 'profit'
							].values[0]
							profit_last_yl = data_use.loc[
								data_use['report_period'] == last_latest, 'profit'
							].values[0]
							profit_ttm = profit_last + profit_last_y - profit_last_yl
					
					except IndexError:
						logger.warning(f"股票{code}在{year}年数据缺失，无法计算TTM")
					
					profit_list.append(profit_ttm)
				
				# 创建结果序列
				profit_df = pd.Series(profit_list, index=all_years, name=code)
				ttm_results.append(profit_df)
			
			# 5. 合并所有结果
			df_ttm = pd.concat(ttm_results, axis=1).T.reset_index()
			df_ttm.rename(columns={'index': 'code'}, inplace=True)
			
			# 6. 重塑为长格式
			df_ttm_long = df_ttm.melt(
				id_vars='code',
				value_vars=all_years,
				var_name='year',
				value_name='profit_ttm'
			)
			df_ttm_long.dropna(subset=['profit_ttm'], inplace=True)
			
			# 7. 添加报告期标识
			df_ttm_long['report_period'] = df_ttm_long['year'].apply(lambda y: f"{y}1231")
			
			logger.info(f"TTM计算完成，有效记录数: {len(df_ttm_long)}")
			
			# 8. 保存TTM数据
			DATA_STORAGE = DataStorage()
			save_path = DATA_STORAGE.save_l1(
				df=df_ttm_long,
				dataset="financials",
				table="ashare_net_profit_ttm_annual",
				date=self.date)
			return save_path
		
		except Exception as e:
			logger.error(f"净利润TTM计算失败: {str(e)}", exc_info=True)
			raise
	
	def preprocess_all(self):
		"""预处理数据"""
		results = {}
		results['ashare_income'] = self.preprocess_ashare_income()
		results['ashare_balancesheet'] = self.preprocess_ashare_balancesheet()
		results['net_profit_ttm'] = self.calculate_net_profit_ttm()
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
	processor = FinancialsPreprocessor(date="20230821")
	# profit_ttm = processor.preprocess_ashare_balancesheet()
	# profit_ttm = processor.preprocess_ashare_income()
	profit_ttm = processor.calculate_net_profit_ttm()
	# processor.preprocess_all()
	# summary = processor.get_processing_summary()
	# print("处理摘要:")
	# for table, info in summary['details'].items():
	# 	print(f"- {table}: {info['rows']}行, 保存至 {info['path']}")
