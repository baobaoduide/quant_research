# -*- coding: utf-8 -*-
"""
Created on Mon Nov 15 14:35:42 2021

@author: Tianyu Xiao
"""

import os.path
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


def read_data():
    path_data = os.path.join(os.path.dirname(__file__), '中欧基金图表模板.xlsx')
    rtn = pd.read_excel(path_data, sheet_name='图表')
    rtn.rename(columns={'亿元': '时间'}, inplace=True)
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
        color_set = str(color_name.split('%')[0] / 100)
    else:
        print('Color name error!')
    return color_set


def draw_plot(bond_data):
    plt.figure()
    plt.plot(bond_data['时间'], bond_data['国债'], label='国债', color=set_color('50% 灰'))
    plt.plot(bond_data['时间'], bond_data['国开债'], label='国开债', color=set_color('50% 灰'))
    plt.plot(bond_data['时间'], bond_data['进出口债'], label='进出口债', color=set_color('40% 灰'))
    plt.plot(bond_data['时间'], bond_data['农发债'], label='农发债', color=set_color('30% 灰'))
    plt.legend()
    pass


def draw_controller():
    bond_data = read_data()
    #
    draw_plot(bond_data)
    pass


draw_controller()
