import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import font_manager
fontP = font_manager.FontProperties(fname="/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages/matplotlib/mpl-data/fonts/ttf/SimHei.ttf")
import platform
sys_platform = platform.platform().lower()
if 'macos' in sys_platform:
    folder = r'/Users/xiaotianyu/Desktop/data/result_202308'
elif 'windows' in sys_platform:
    folder = r'E:\data_gk_model\result_202308'


def plot_data(name):
    path_data = os.path.join(folder, '拆分结果_' + name + '.csv')
    data = pd.read_csv(path_data, index_col=['年份'], encoding='gbk')
    data_display = data[['实际收益', '股息驱动', '业绩驱动', '估值驱动']]
    #
    part_df = data[['股息驱动', '业绩驱动', '估值驱动']].fillna(0)
    colors = ['orange', 'red', 'blue']
    names = part_df.columns.to_list()
    names_label = ['delta_div', 'delta_profit', 'delta_pe']
    title = name
    xlabel_ = data.index.to_list()
    part_df_a = part_df.T.to_numpy()

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
    #
    fig, ax = plt.subplots(1, figsize=(10, 6))
    for idx, name_h in enumerate(names):
        plt.bar(xlabel_, part_df[name_h], bottom=data_stack[idx], color=colors[idx], width=0.3, label=names_label[idx])
    plt.plot(xlabel_, data['实际收益'], color='black', marker='.', linewidth=1, label='actual_ret')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_color('gray')
    # plt.title(title, fontsize=16)
    plt.xticks(xlabel_)
    ax.set_ylabel('percent_chg (%)')
    ax.set_xlabel('year')
    plt.legend()
    path_save_fig = os.path.join(folder, '拆分结果_' + name + '.png')
    plt.savefig(path_save_fig)
    plt.close()
    plt.show()
    return data_display


if __name__ == '__main__':
    # plot_data('沪深300')
    stock_list = ['东方财富', '贵州茅台', '恒瑞医药']
    broad_index_list = ['沪深300', '中证500', '上证50', '创业板指', '红利指数']
    ind_index_list = [
        "石油石化",
        "煤炭",
        "有色金属",
        "电力及公用事业",
        "钢铁",
        "基础化工",
        "建筑",
        "建材",
        "轻工制造",
        "机械",
        "电力设备及新能源",
        "国防军工",
        "汽车",
        "商贸零售",
        "消费者服务",
        "家电",
        "纺织服装",
        "医药",
        "食品饮料",
        "农林牧渔",
        "银行",
        "非银行金融",
        "房地产",
        "交通运输",
        "电子",
        "通信",
        "计算机",
        "传媒",
        "综合",
        "综合金融",
    ]
    all_list = stock_list + broad_index_list + ind_index_list
    for name in all_list:
        print(name)
        plot_data(name)
