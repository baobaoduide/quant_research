import os
import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = 'SimHei'
import warnings
warnings.filterwarnings('ignore')
folder_raw = r'D:\code_workspace\data_warehouse\L0_raw_data\wind\cb_section'
folder_pre = r'D:\code_workspace\data_warehouse\projects\track_convertible_bond'


def label_cb_type(pdyjl):
       if pdyjl > 20:
              type = '偏股型'
       elif pdyjl > -20:
              type = '平衡型'
       else:
              type = '偏债型'
       return type


def cal_section_data(date):
       """针对某一日的截面数据计算衍生向量"""
       date_num = date.replace('-', '')
       path_load = os.path.join(folder_raw, 'cb_section_' + date_num + '.xlsx')
       df = pd.read_excel(path_load)
       df['平价底价溢价率'] = (df['转换价值'] / df['纯债价值'] - 1) * 100
       df['类型'] = df['平价底价溢价率'].apply(label_cb_type)
       bins_parity = [0, 80, 90, 100, 110, 120, 130, df['转换价值'].max()]
       labels_parity = ['80以下', '80-90', '90-100', '100-110', '110-120', '120-130', '130以上']
       df['平价分类'] = pd.cut(df['转换价值'], bins_parity, labels=labels_parity)
       df['总市值'] /= 100000000
       bins_mv = [0, 30, 50, 100, 500, df['总市值'].max()]
       labels_mv = ['30亿以下', '30-50亿', '50-100亿', '100-500亿', '500亿以上']
       df['正股市值分类'] = pd.cut(df['总市值'], bins_mv, labels=labels_mv)
       path_save = os.path.join(folder_pre, 'cb_section_pre_' + date_num + '.xlsx')
       df.to_excel(path_save, index=False)
       return df


def curve_func(x, a, b):
       """百元平价溢价率模型"""
       return a + b / x


def fit_100parity(df):
       """百元平价溢价率拟合计算函数"""
       # 拟合百元平价溢价率
       df_reg = df[['转换价值', '转股溢价率']].dropna(how='any')
       params, params_covariance = curve_fit(f=curve_func, xdata=df_reg['转换价值'], ydata=df_reg['转股溢价率'])
       a_fit, b_fit = params
       y_pred = curve_func(df_reg['转换价值'], a_fit, b_fit)
       residuals = df_reg['转股溢价率'] - y_pred
       ss_res = np.sum(residuals ** 2)
       ss_tot = np.sum((df_reg['转股溢价率'] - np.mean(df_reg['转股溢价率'])) ** 2)
       r_squared = 1 - (ss_res / ss_tot)
       # print(f"R² = {r_squared:.4f}")
       # plt.scatter(df_reg['转换价值'], df_reg['转股溢价率'], label='原始数据', color='blue')
       # x_fit = np.linspace(min(df_reg['转换价值']), max(df_reg['转换价值']), 100)
       # y_fit = curve_func(x_fit, a_fit, b_fit)
       # plt.plot(x_fit, y_fit, 'r-', label='拟合曲线')
       # plt.xlabel('转换价值')
       # plt.ylabel('转股溢价率')
       # plt.title('非线性拟合: y = a + b*(1/x)')
       # plt.legend()
       # plt.show()
       return curve_func(100, a_fit, b_fit)


def cal_conv_pre(df):
       return (df['收盘价'] * df['债券余额']).sum() / (df['转换价值'] * df['债券余额']).sum() - 1


def cal_weighted_price(df):
       return (df['收盘价'] * df['债券余额']).sum() / df['债券余额'].sum()


def cal_indicators(date):
       """计算某一日的截面数据汇总的单个指标"""
       date_num = date.replace('-', '')
       path_save = os.path.join(folder_pre, 'cb_section_pre_' + date_num + '.xlsx')
       df = pd.read_excel(path_save)
       #
       # 计算分组转股溢价率指标
       conv_pre_cate_parity = df.groupby(by=['平价分类']).apply(cal_conv_pre)
       conv_pre_cate_bs = df.groupby(by=['类型']).apply(cal_conv_pre)
       conv_pre_cate_mv = df.groupby(by=['正股市值分类']).apply(cal_conv_pre)
       # 计算分组价格指标
       price_cate_mv = df.groupby(by=['正股市值分类']).apply(cal_weighted_price)
       price_cate_ind = df.groupby(by=['所属申万一级行业']).apply(cal_weighted_price)
       # 计算截面整体指标
       median_close = df['收盘价'].median()
       median_ytm = df['纯债到期收益率'].median()
       weight_average_parity = (df['债券余额'] * df['转换价值']).sum() / df['债券余额'].sum()
       parity_premium_100 = fit_100parity(df)
       day_sum_ = pd.Series([median_close, weight_average_parity, parity_premium_100, median_ytm], index=['收盘价中位数', '加权平均平价', '百元平价拟合溢价率', '纯债到期收益率中位数'])
       #
       # 整合以上结果
       result = pd.concat([conv_pre_cate_parity, conv_pre_cate_bs, conv_pre_cate_mv, price_cate_mv, price_cate_ind, day_sum_], keys=['转股溢价率']*3+['价格']*2+['整体指标'])
       return result, day_sum_


if __name__ == '__main__':
       date = "2025-08-05"
       cal_section_data(date)
       result = cal_indicators(date)
       print(date, '\n', result[1])
       #

