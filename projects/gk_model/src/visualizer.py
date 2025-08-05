import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from core.config import Paths
from core.data_utils.loader import DataLoader

# 设置全局字体
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


class GKResultVisualizer:
	"""GK模型结果可视化器（优化版）"""
	
	def __init__(self, project="gk_model"):
		self.project = project
		
		# 设置结果目录和图片子目录
		paths = Paths()
		self.result_dir = Path(paths.get_l3_results_path(project))
		self.figure_dir = self.result_dir / "decomposition_figures"
		
		# 确保目录存在
		self.figure_dir.mkdir(parents=True, exist_ok=True)
		
		# 初始化数据加载器
		self.data_loader = DataLoader()
	
	def plot_drivers_stacked_bar(self, df, index_name):
		"""
		绘制收益驱动成分堆积柱状图（包含实际收益标签）

		:param df: 分解结果DataFrame
		:param index_name: 指数名称
		:return: 图像路径
		"""
		# 筛选指定指数的数据
		df_index = df[df['index_name'] == index_name].sort_values('年份')
		
		if df_index.empty:
			print(f"未找到指数 {index_name} 的数据")
			return None
		
		# 准备绘图数据
		data = df_index.set_index('年份')
		
		# 填充NaN值为0
		part_df = data[['股息驱动', '业绩驱动', '估值驱动']].fillna(0)
		colors = ['orange', 'red', 'blue']
		names = part_df.columns.to_list()
		title = index_name
		xlabel_ = data.index.astype(str).tolist()  # 确保年份为字符串
		
		# 转换为numpy数组
		part_df_a = part_df.T.to_numpy()
		
		# 计算累积值（用于堆叠柱状图）
		def get_cumulated_array(data, **kwargs):
			cum = data.clip(**kwargs)
			cum = np.cumsum(cum, axis=0)
			d = np.zeros(np.shape(data))
			d[1:] = cum[:-1]
			return d
		
		cumulated_data = get_cumulated_array(part_df_a, min=0)
		cumulated_data_neg = get_cumulated_array(part_df_a, max=0)
		row_mask = part_df_a < 0
		cumulated_data[row_mask] = cumulated_data_neg[row_mask]
		data_stack = cumulated_data
		
		# 创建图表
		fig, ax = plt.subplots(1, figsize=(10, 6))
		
		# 绘制堆叠柱状图
		for idx, name_h in enumerate(names):
			plt.bar(xlabel_, part_df[name_h], bottom=data_stack[idx],
			        color=colors[idx], width=0.3, label=name_h)
		
		# 绘制实际收益点线图
		plt.plot(xlabel_, data['实际收益'], color='black', marker='.',
		         linewidth=1, label='实际收益')
		
		# 添加实际收益数据标签
		for i, year in enumerate(xlabel_):
			value = data['实际收益'].iloc[i]
			# 根据正负值决定标签位置
			offset = 0.05 if value >= 0 else -0.08
			# 格式化标签为百分比
			label_text = f"{value:.2%}" if abs(value) < 1 else f"{value:.1%}"
			plt.text(i, value + offset, label_text,
			         ha='center', va='bottom', fontsize=9)
		
		# 设置图表样式
		ax.spines['right'].set_visible(False)
		ax.spines['top'].set_visible(False)
		ax.spines['bottom'].set_color('gray')
		
		# 添加标题和标签
		plt.title(title, fontsize=16)
		plt.xticks(xlabel_)
		ax.set_ylabel('百分比 (%)')
		plt.legend()
		
		# 保存图像
		img_name = f"拆分结果_{index_name}.png"
		img_path = self.figure_dir / img_name
		plt.savefig(img_path, dpi=300, bbox_inches='tight')
		plt.close()
		
		print(f"图像已保存至: {img_path}")
		return img_path
	
	def load_decomposition_data(self, result_name="gk_decomposition_all"):
		"""
		使用DataLoader加载分解结果数据
		"""
		# 假设DataLoader有一个load_excel方法用于加载xlsx文件
		return self.data_loader.load_l3(
			project=self.project,
			result_name=result_name,
			file_format="xlsx"
		)
	
	def plot_all_indices(self, result_name="gk_decomposition_all"):
		"""为所有指数生成收益分解图（使用DataLoader加载数据）"""
		# 加载数据
		df = self.load_decomposition_data(result_name)
		
		if df is None or df.empty:
			print("无法加载分解结果")
			return []
		
		image_paths = []
		
		for index_name in df['index_name'].unique():
			img_path = self.plot_drivers_stacked_bar(df, index_name)
			if img_path:
				image_paths.append(img_path)
		
		print(f"生成了 {len(image_paths)} 个指数的分解图")
		return image_paths
	
	def create_summary_report(self, image_paths, result_name="gk_decomposition_summary"):
		"""创建汇总报告（包含所有图表）"""
		if not image_paths:
			print("没有图像，无法创建报告")
			return None
		
		# 创建HTML报告
		report_name = f"{result_name}.html"
		report_path = self.result_dir / report_name
		
		with report_path.open('w', encoding='utf-8') as f:
			f.write("<html><head><meta charset='UTF-8'><title>GK模型收益分解报告</title></head><body>")
			f.write(f"<h1>GK模型收益分解报告</h1>")
			f.write(f"<p>项目: {self.project}</p>")
			f.write(f"<p>包含指数数量: {len(image_paths)}</p>")
			
			# 添加所有图像
			for img_path in image_paths:
				img_name = img_path.name
				index_name = img_name.replace("拆分结果_", "").replace(".png", "")
				relative_img_path = Path("decomposition_figures") / img_name
				
				f.write(f"<h2>{index_name}</h2>")
				f.write(f"<img src='{relative_img_path}' width='800'><br>")
			
			f.write("</body></html>")
		
		print(f"报告已生成: {report_path}")
		return report_path


def main():
	"""主函数：批量处理所有指数并生成报告"""
	# 初始化可视化器
	visualizer = GKResultVisualizer(project="gk_model")
	
	# 加载数据并绘制所有指数
	image_paths = visualizer.plot_all_indices()
	
	# 生成汇总报告
	visualizer.create_summary_report(image_paths)


if __name__ == '__main__':
	main()