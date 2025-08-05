from core.data_utils.loader import DataLoader
from core.data_utils.storage import DataStorage
import pandas as pd
import numpy as np
from datetime import datetime


class GKDataPreprocessor:
    """GK模型数据预处理类"""

    def __init__(self, data_date="20230821", index_config=None):
        """
        初始化预处理类

        :param data_date: 数据日期，格式为YYYYMMDD
        :param index_config: 指数配置字典，格式为:
            {
                '指数名称1': {
                    'code': '指数代码1',
                    'table': '数据表名'
                },
                '指数名称2': {
                    'code': '指数代码2',
                    'table': '数据表名'
                }
            }
        """
        self.data_loader = DataLoader()
        self.data_storage = DataStorage()
        self.data_date = data_date  # 数据日期

        # 配置常量
        self.start_year = 2009
        self.end_year = datetime.now().year

        # 默认指数配置（如果未提供）
        self.index_config = index_config or self.default_index_config()

    def default_index_config(self):
        """返回默认指数配置"""
        return {
            '沪深300': {'code': '000300.SH', 'table': 'aindex_members'},
            '中证500': {'code': '000905.SH', 'table': 'aindex_members'},
            '上证50': {'code': '000016.SH', 'table': 'aindex_members'},
            '创业板指': {'code': '399006.SZ', 'table': 'aindex_members'},
            '红利指数': {'code': '000015.SH', 'table': 'aindex_members'},
            '石油石化': {'code': 'CI005001.WI', 'table': 'aindex_members_citics'},
            '煤炭': {'code': 'CI005002.WI', 'table': 'aindex_members_citics'},
            '有色金属': {'code': 'CI005003.WI', 'table': 'aindex_members_citics'}
        }

    def add_index(self, index_name, index_code, table_name):
        """添加自定义指数"""
        self.index_config[index_name] = {
            'code': index_code,
            'table': table_name
        }

    def load_stock_data(self):
        """加载并处理个股基础数据"""
        print("加载个股基础数据...")

        # 加载数据
        income = self.data_loader.load_l1(dataset='financials', date=self.data_date, table='ashare_net_profit_ttm_annual')
        prices = self.data_loader.load_l1(dataset='market', date=self.data_date, table='ashare_eod_derivativeindicator')
        dividend = self.data_loader.load_l1(dataset='corporate_actions', date=self.data_date, table='ashare_dividend')

        # 处理市值数据
        prices = prices.rename(columns={'market_value': '总市值'})
        prices['年份'] = prices['date'].str[:4].astype(int)
        mv_year = prices.groupby(['code', '年份'])['总市值'].last().reset_index()

        # 处理分红数据
        dividend['分红市值'] = dividend['cash_div_ps_pre'] * dividend['base_share']
        dividend['年份'] = dividend['date'].str[:4].astype(int)
        div_year = dividend.groupby(['code', '年份'])['分红市值'].sum().reset_index()

        # 处理利润数据
        income = income.rename(columns={'year': '年份', 'profit_ttm': '净利润_TTM'})

        # 合并数据
        stock_data = mv_year.merge(div_year, on=['code', '年份'], how='outer')
        stock_data = stock_data.merge(income[['code', '年份', '净利润_TTM']], on=['code', '年份'], how='outer')

        # 筛选年份范围
        stock_data = stock_data[(stock_data['年份'] > self.start_year) & (stock_data['年份'] <= self.end_year)]

        return stock_data

    def save_stock_data(self, stock_data):
        """保存个股基础数据"""
        save_path = self.data_storage.save_l2(
            df=stock_data,
            project="gk_model",
            table="ashare_base_info",
            file_format='parquet'
        )
        print(f"个股数据已保存至: {save_path}")
        return save_path

    def load_index_components(self, index_info, year):
        """
        加载指数成分股

        :param index_info: 指数信息字典，包含'code'和'table'
        :param year: 年份
        :return: 成分股代码列表
        """
        year_end_date = f"{year}-12-31"  # 年末日期

        try:
            # 加载成分股数据
            components = self.data_loader.load_l1(
                dataset='index_data',
                date=self.data_date,  # 使用年末日期作为数据版本
                table=index_info['table']
            )

            # 过滤特定指数
            components = components[components['code_index'] == index_info['code']]

            # 转换为日期格式
            components['date_in'] = pd.to_datetime(components['date_in'])
            components['date_out'] = pd.to_datetime(components['date_out'])
            year_end = pd.Timestamp(year_end_date)

            # 过滤有效成分股
            # 1. 纳入日期在年末之前
            # 2. 剔除日期在年末之后或为空
            valid_components = components[
                (components['date_in'] <= year_end) &
                (components['date_out'].isna() | (components['date_out'] > year_end))
                ]

            return valid_components['code'].unique().tolist()
        except Exception as e:
            print(f"加载指数成分股失败: {index_info['code']} {year}年, 错误: {str(e)}")
            return []

    def aggregate_index_data(self, stock_data):
        """聚合指数数据"""
        print("\n开始聚合指数数据...")
        index_results = []

        for index_name, index_info in self.index_config.items():
            print(f"处理指数: {index_name} ({index_info['code']})")

            for year in range(self.start_year + 1, self.end_year + 1):
                # 获取成分股
                components = self.load_index_components(index_info, year)
                if not components:
                    print(f"  {year}年无有效成分股数据，跳过")
                    continue

                # 获取成分股当年数据
                year_data = stock_data[
                    (stock_data['code'].isin(components)) &
                    (stock_data['年份'] == year)
                    ].copy()

                # 填充缺失值
                year_data.fillna(0, inplace=True)

                # 聚合数据
                agg_data = {
                    'index_name': index_name,
                    'index_code': index_info['code'],
                    '年份': year,
                    '成分股数量': len(components),
                    '总市值': year_data['总市值'].sum(),
                    '分红市值': year_data['分红市值'].sum(),
                    '净利润_TTM': year_data['净利润_TTM'].sum()
                }

                # 计算PE
                if agg_data['净利润_TTM'] > 0:
                    agg_data['PE_TTM'] = agg_data['总市值'] / agg_data['净利润_TTM']
                else:
                    agg_data['PE_TTM'] = np.nan

                index_results.append(agg_data)

        return pd.DataFrame(index_results)

    def save_index_data(self, index_data):
        """保存指数聚合数据"""
        save_path = self.data_storage.save_l2(
            df=index_data,
            project="gk_model",
            table="index_aggregated_info",
            file_format='parquet'
        )
        print(f"指数数据已保存至: {save_path}")
        return save_path

    def run(self):
        """运行完整的预处理流程"""
        print("=" * 50)
        print(f"开始GK模型数据预处理 [数据日期: {self.data_date}]")
        print(f"处理指数数量: {len(self.index_config)}")
        print("=" * 50)

        # 步骤1: 准备个股基础数据
        stock_data = self.load_stock_data()
        print(f"加载完成，共 {len(stock_data)} 条个股记录")

        # 步骤2: 保存个股数据
        stock_path = self.save_stock_data(stock_data)

        # 步骤3: 聚合并保存指数数据
        index_data = self.aggregate_index_data(stock_data)
        index_path = self.save_index_data(index_data)

        print("\n" + "=" * 50)
        print("预处理完成!")
        print(f"个股数据: {stock_path}")
        print(f"指数数据: {index_path}")
        print(f"处理了 {len(self.index_config)} 个指数")
        print(f"覆盖年份: {self.start_year + 1}-{self.end_year}")
        print(f"生成的指数数据记录: {len(index_data)} 条")
        print("=" * 50)

        return {
            'stock_data': stock_data,
            'index_data': index_data,
            'stock_path': stock_path,
            'index_path': index_path
        }


if __name__ == '__main__':
    # 创建预处理器实例
    preprocessor = GKDataPreprocessor(data_date="20230821")

    # 运行预处理流程
    results = preprocessor.run()

    # 显示部分结果
    print("\n个股数据示例:")
    print(results['stock_data'].head())

    print("\n指数数据示例:")
    print(results['index_data'].head())
    