import os
import pandas as pd


# folder = r'E:\icbcaxa\work_icbcaxa\学习小组\转债相关'
folder = r'/Users/xiaotianyu/Desktop/学习小组/转债相关'
path_trade = os.path.join(folder, 'trade_20190101to20231130.xlsx')
trade_df = pd.read_excel(path_trade, sheet_name='转债数据', dtype={'证券代码': str})
trade_df['direction'] = trade_df['业务类型'].apply(func=lambda x: 1 if x in ['新债(上市流通)', '债券买入'] else -1)
trade_df['成交数量'] = trade_df['成交数量'] * trade_df['direction']
secus = trade_df['证券代码'].drop_duplicates().reset_index(drop=True)
trade_df['实际清算金额'] = trade_df.apply(func=lambda x: -x['成交金额'] if x['业务类型'] == '新债(上市流通)' else x['实际清算金额'], axis=1)
#
done_df = []
no_done_code = []
for i, code in enumerate(secus):
    trade_df_i = trade_df[trade_df['证券代码'] == code]
    trade_df_i.reset_index(drop=True, inplace=True)
    sec_name = trade_df_i['证券名称'].iloc[0]
    print(i, code, sec_name)
    num_hold = trade_df_i['成交数量'].sum()
    print(num_hold)
    if (num_hold == 0) or (sec_name == '光大转债'):
        start_d = trade_df_i['日期'][0]
        end_d = trade_df_i['日期'].iloc[-1]
        cost = -trade_df_i[trade_df_i['实际清算金额'] < 0]['实际清算金额'].sum()
        ret_ = trade_df_i['实际清算金额'].sum()
        in_fp = 0
        done_data = pd.Series([code, sec_name, start_d, end_d, cost, ret_, in_fp])
        done_df.append(done_data)
    else:
        position_data = trade_df_i[['日期', '组合', '证券代码', '证券名称', '业务类型', '成交数量', '实际清算金额', '结转成本']]
        use_df = position_data[position_data['业务类型'] == '债券卖出']
        if len(use_df) > 0:
            start_d = trade_df_i["日期"][0]
            end_d = use_df["日期"].iloc[-1]
            cost = use_df["结转成本"].sum()
            ret_ = (use_df["实际清算金额"] - use_df["结转成本"]).sum()
            if num_hold < 0:
                in_fp = 0
            else:
                in_fp = 1
            done_data = pd.Series([code, sec_name, start_d, end_d, cost, ret_, in_fp])
            done_df.append(done_data)
        else:
            no_done_code.append(code)
done_data_all = pd.concat(done_df, axis=1).T
done_data_all.columns = ['证券代码', '证券名称', '初次持有日期', '最后持有日期', '累计成本', '累计收益', '是否还有持仓']
done_data_all['模糊收益率'] = done_data_all['累计收益'] / done_data_all['累计成本']
done_data_all.sort_values(by=['是否还有持仓', '初次持有日期'], inplace=True)
path_folder = os.path.join(folder, '转债实现收益.xlsx')
done_data_all.to_excel(path_folder, index=False)
