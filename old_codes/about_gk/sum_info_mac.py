import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import platform
from matplotlib import font_manager
fontP = font_manager.FontProperties(fname="/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages/matplotlib/mpl-data/fonts/ttf/SimHei.ttf")
sys_platform = platform.platform().lower()
if 'macos' in sys_platform:
    folder = r'/Users/xiaotianyu/Desktop/data/result_202308'
elif 'windows' in sys_platform:
    folder = r'E:\data_gk_model\result_202308'


def sum_data():
    stock_list = ['东方财富', '贵州茅台', '恒瑞医药']
    broad_index_list = ['沪深300', '中证500', '上证50', '创业板指', '红利指数']
    index_list = ["石油石化", "煤炭", "有色金属", "电力及公用事业", "钢铁", "基础化工", "建筑", "建材", "轻工制造", "机械", "电力设备及新能源", "国防军工", "汽车", "商贸零售", "消费者服务", "家电", "纺织服装", "医药", "食品饮料", "农林牧渔", "银行", "非银行金融", "房地产", "交通运输", "电子", "通信", "计算机", "传媒", "综合", "综合金融"]
    path_data = os.path.join(folder, '运行结果整合.xlsx')
    writer = pd.ExcelWriter(path_data)
    r_num = 1
    for name in stock_list:
        print(name)
        path_data = os.path.join(folder, '拆分结果_' + name + '.csv')
        data = pd.read_csv(path_data, index_col=['年份'], encoding='gbk')
        data_display = data.copy()
        data_display.loc['累计'] = data_display.sum()
        data_display.loc['年均'] = data_display.loc['累计'] / len(data)
        data_display.to_excel(writer, sheet_name='个股', startrow=r_num)
        writer.sheets["个股"].cell(row=r_num, column=1, value=name)
        # writer.sheets['个股'].write_string(r_num-1, 0, name)
        r_num += len(data_display) + 3

    r_num_index = 1
    for name in broad_index_list:
        print(name)
        path_data = os.path.join(folder, '拆分结果_' + name + '.csv')
        data = pd.read_csv(path_data, index_col=['年份'], encoding='gbk')
        data_display = data.copy()
        data_display.loc['累计'] = data_display.sum()
        data_display.loc['年均'] = data_display.loc['累计'] / len(data)
        data_display.to_excel(writer, sheet_name='宽基指数', startrow=r_num_index)
        writer.sheets["宽基指数"].cell(row=r_num_index, column=1, value=name)
        # writer.sheets['宽基指数'].write_string(r_num_index-1, 0, name)
        r_num_index += len(data_display) + 3

    r_num_index = 1
    for name in index_list:
        print(name)
        path_data = os.path.join(folder, "拆分结果_" + name + ".csv")
        data = pd.read_csv(path_data, index_col=["年份"], encoding="gbk")
        data_display = data.copy()
        data_display.loc["累计"] = data_display.sum()
        data_display.loc["年均"] = data_display.loc["累计"] / len(data)
        data_display.to_excel(writer, sheet_name="中信一级行业指数", startrow=r_num_index)
        writer.sheets["中信一级行业指数"].cell(row=r_num_index, column=1, value=name)
        # writer.sheets["中信一级行业指数"].write_string(r_num_index - 1, 0, name)
        r_num_index += len(data_display) + 3
    writer._save()
    return


def sum_info2(s_y, e_y):
    index_list = ["沪深300", "中证500", "上证50", "创业板指", "红利指数", "石油石化", "煤炭", "有色金属", "电力及公用事业", "钢铁", "基础化工", "建筑", "建材", "轻工制造", "机械", "电力设备及新能源", "国防军工", "汽车", "商贸零售", "消费者服务", "家电", "纺织服装", "医药", "食品饮料", "银行", "非银行金融", "房地产", "交通运输", "电子", "通信", "计算机", "传媒", "综合", "综合金融"]
    sum_df_list = []
    for name in index_list:
        print(name)
        path_data = os.path.join(folder, "拆分结果_" + name + ".csv")
        data = pd.read_csv(path_data, index_col=["年份"], encoding="gbk")
        data = data.loc[s_y: e_y]
        data_sum = data.sum().rename(name)
        sum_df_list.append(data_sum)
    sum_df = pd.concat(sum_df_list, axis=1).T
    path_ = r'/Users/xiaotianyu/Desktop/data/result_202308/宽基及行业指数汇总结果_'+str(s_y)+'to'+str(e_y)+'.xlsx'
    sum_df.to_excel(path_)
    #
    sum_df_use = sum_df.sort_values(by=['实际收益'], ascending=False)
    part_df = sum_df_use[["股息驱动", "业绩驱动", "估值驱动"]]
    colors = ["orange", "red", "blue"]
    names = part_df.columns.to_list()
    names_label = ["delta_div", "delta_profit", "delta_pe"]
    xlabel_ = sum_df_use.index.to_list()
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
        plt.bar(
            xlabel_,
            part_df[name_h],
            bottom=data_stack[idx],
            color=colors[idx],
            width=0.3,
            label=names_label[idx],
        )
    plt.scatter(
        xlabel_, sum_df_use["实际收益"], color="black", marker=".", linewidth=1, label="actual_ret"
    )
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["bottom"].set_color("gray")
    # plt.title(title, fontsize=16)
    plt.xticks(xlabel_, fontproperties=fontP, rotation='vertical')
    ax.set_ylabel("percent (%)")
    plt.legend()
    plt.tight_layout()
    path_save_fig = os.path.join(folder, '指数分解汇总图_'+str(s_y)+'to'+str(e_y)+'.png')
    plt.savefig(path_save_fig)
    plt.close()
    plt.show()
    pass


if __name__ == '__main__':
    for y in [2011, 2017, 2020]:
        sum_info2(y, 2022)
    # sum_data()
