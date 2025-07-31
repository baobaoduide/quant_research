import os.path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, MonthLocator
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


def read_data(start_time, end_time):
    path_data = os.path.join(os.path.dirname(__file__), '中欧基金图表模板.xlsx')
    rtn = pd.read_excel(path_data, sheet_name='图表')
    rtn.rename(columns={'亿元': '时间'}, inplace=True)
    rtn['时间'] = rtn['时间'].apply(func=lambda x: x.replace(day=1))
    month = rtn['时间'].dt.strftime('%Y/%m')
    rtn = rtn[(month >= start_time) & (month <= end_time)].set_index('时间')
    return rtn


def multi_color_code(color_rgb_tuple):
    start_mark = '#'
    color_ = start_mark + str(hex(color_rgb_tuple[0])[2:].zfill(2)) + str(hex(color_rgb_tuple[1])[2:].zfill(2)) + str(hex(color_rgb_tuple[2])[2:].zfill(2))
    return color_


def set_color(color_name):
    color_dict = {'中欧蓝': multi_color_code((8, 68, 152)), '深蓝': multi_color_code((16, 74, 164)), '群青': multi_color_code((40, 92, 174)), '浅蓝': multi_color_code((160, 182, 218)), '钴蓝': multi_color_code((111, 147, 201)), '蓝白': multi_color_code((207, 219, 237)), '淡蓝': multi_color_code((255, 237, 246)), '中欧橙': multi_color_code((255, 162, 2)), '淡黄': multi_color_code((255, 236, 212)), '中欧金': multi_color_code((165, 140, 105)), '中欧浅金': multi_color_code((185, 163, 138))}
    if color_name in color_dict.keys():
        color_set = color_dict[color_name]
    elif ('%' in color_name) & ('灰' in color_name):
        color_set = str(1 - float(color_name.split('%')[0]) / 100)
    else:
        print('Color name error!')
    return color_set


def draw_plot(bond_data, fig_size=(9, 6), title_name='', ylabel='规模(亿元)', xlabel='时间', columns_use=['国债', '国开债', '进出口债', '农发债'], col_colors=['群青', '浅蓝', '中欧橙', '80% 灰']):
    ax_color = set_color('80% 灰')
    fig, ax = plt.subplots(figsize=fig_size)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color(set_color('30% 灰'))
    ax.spines['left'].set_color(ax_color)
    for i in range(len(columns_use)):
        column_this = columns_use[i]
        ax.plot(bond_data.index, bond_data[column_this], label=column_this, lw=1.5, color=set_color(col_colors[i]))
    ax.set_ylim(0)
    l = ax.legend()
    for text in l.get_texts():
        text.set_color(ax_color)
    ax.tick_params(axis='y', labelsize=9, color=ax_color, labelcolor=ax_color)
    ax.tick_params(axis='x', labelsize=9, color=ax_color, labelcolor=ax_color)
    ax.set_title(title_name, color=ax_color)
    ax.set_ylabel(ylabel, color=ax_color)
    ax.set_xlabel(xlabel, color=ax_color)
    plt.show()
    pass


def draw_pie(bond_data, fig_size=(9, 6), title_name='', columns_use=['国债', '国开债', '进出口债', '农发债'], col_colors=['群青', '浅蓝', '中欧橙', '50% 灰']):
    bond_data_use = bond_data[columns_use]
    data_labels = list(bond_data_use.index)
    #
    def func(pct, allvals):
        absolute = int(round(pct / 100. * np.sum(allvals)))
        return "{:.2f}%\n({:d}亿元)".format(pct, absolute)
    #
    ax_color = set_color('80% 灰')
    fig, ax = plt.subplots(figsize=fig_size)
    wedges, texts, autotexts = ax.pie(bond_data_use, autopct=lambda pct: func(pct, bond_data_use), colors=[set_color(i) for i in col_colors])
    l = ax.legend(wedges, data_labels,
              loc='center left',
              bbox_to_anchor=(1, 0, 0.5, 1))
    for text in l.get_texts():
        text.set_color(ax_color)
    for i in autotexts:
        i.set_color(ax_color)
    ax.set_title(title_name, color=ax_color)
    plt.show()
    pass


def draw_stackplot(bond_data, fig_size=(9, 6), title_name='', ylabel='规模(亿元)', xlabel='时间', columns_use=['国债', '国开债', '进出口债', '农发债'], col_colors=['群青', '浅蓝', '中欧橙', '50% 灰']):
    bond_data_use = bond_data[columns_use]
    #
    ax_color = set_color('80% 灰')
    fig, ax = plt.subplots(figsize=fig_size)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color(set_color('30% 灰'))
    ax.spines['left'].set_color(ax_color)
    ax.stackplot(bond_data_use.index, bond_data_use.T,
                 labels=bond_data_use.columns, colors=[set_color(color) for color in col_colors])
    ax.legend(loc='upper left')
    l = ax.legend()
    for text in l.get_texts():
        text.set_color(ax_color)
    ax.tick_params(axis='y', labelsize=9, color=ax_color, labelcolor=ax_color)
    ax.tick_params(axis='x', labelsize=9, color=ax_color, labelcolor=ax_color)
    ax.set_xticklabels(bond_data_use.index, rotation=45)
    formatter = DateFormatter('%Y/%m')
    ax.xaxis.set_major_formatter(formatter)
    ax.set_title(title_name, color=ax_color)
    ax.set_ylabel(ylabel, color=ax_color)
    ax.set_xlabel(xlabel, color=ax_color)
    plt.show()
    pass


def draw_stacked_bar(bond_data, fig_size=(9, 6), title_name='', ylabel='规模(亿元)', xlabel='时间', columns_use=['国债', '国开债'], col_colors=['群青', '中欧橙']):
    bond_data.index = pd.Series(bond_data.index).dt.strftime('%Y/%m')
    bond_data_use = bond_data[columns_use]
    #
    bar_width = 0.4
    ax_color = set_color('80% 灰')
    ax = bond_data_use.plot.bar(stacked=True, figsize=fig_size, width=bar_width, color=[set_color(i) for i in col_colors])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color(set_color('30% 灰'))
    ax.spines['left'].set_color(ax_color)
    ax.set_ylabel(ylabel, color=ax_color)
    ax.set_xlabel(xlabel, color=ax_color)
    ax.set_title(title_name, color=ax_color)
    ax.tick_params(axis='y', labelsize=9, color=ax_color, labelcolor=ax_color)
    ax.tick_params(axis='x', labelsize=9, color=ax_color, labelcolor=ax_color)
    ax.set_xticklabels(bond_data_use.index, rotation=45)
    l = ax.legend()
    for text in l.get_texts():
        text.set_color(ax_color)
    plt.show()
    pass


def draw_grouped_bar(bond_data, fig_size=(9, 6), title_name='', ylabel='规模(亿元)', xlabel='时间', columns_use=['国债', '国开债', '进出口债', '农发债'], col_colors=['群青', '浅蓝', '中欧橙', '50% 灰']):
    bond_data_use = bond_data[columns_use]
    #
    x = np.arange(len(bond_data_use))
    bwidth = 0.15
    ax_color = set_color('80% 灰')
    ax = bond_data_use.plot.bar(figsize=fig_size, color=[set_color(i) for i in col_colors])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color(set_color('30% 灰'))
    ax.spines['left'].set_color(ax_color)
    ax.set_ylabel(ylabel, color=ax_color)
    ax.set_xlabel(xlabel, color=ax_color)
    ax.set_xticks(x)
    xlab = pd.Series(bond_data.index).dt.strftime('%Y/%m')
    ax.set_xticklabels(xlab, rotation=45)
    ax.set_title(title_name, color=ax_color)
    ax.tick_params(axis='y', labelsize=9, color=ax_color, labelcolor=ax_color)
    ax.tick_params(axis='x', labelsize=9, color=ax_color, labelcolor=ax_color)
    l = ax.legend()
    for text in l.get_texts():
        text.set_color(ax_color)
    plt.show()
    pass


start_time = '2005/01'
end_time = '2019/12'
bond_data = read_data(start_time, end_time)
fig_size = (9, 6)
title_name_plot = start_time + '-' + end_time
y_label_name = '规模(亿元)'
x_label_name = '时间'
columns_use = ['国债', '国开债', '进出口债', '农发债']
col_colors = ['群青', '浅蓝', '中欧橙', '80% 灰']
draw_plot(bond_data)
#
time_pie = '2019/12'
title_name_pie = time_pie
bond_data = read_data(time_pie, time_pie).squeeze()
draw_pie(bond_data)
#
start_time = '2005/01'
end_time = '2005/12'
bond_data = read_data(start_time, end_time)
draw_stackplot(bond_data)
#
start_time = '2005/01'
end_time = '2005/12'
bond_data = read_data(start_time, end_time)
draw_stacked_bar(bond_data)
#
start_time = '2005/01'
end_date = '2006/05'
bond_data = read_data(start_time, end_time)
draw_grouped_bar(bond_data)
