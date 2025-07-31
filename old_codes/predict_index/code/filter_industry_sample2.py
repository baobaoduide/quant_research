import os
import pandas as pd
from load_data_from_data_base import load_index_list, load_aindexmembers, load_main_business, load_industry_cate_zz, load_wind_industry
from util_common import check_index
path_folder = r'H:\database_local\stock\temp_predict_indexmem\板块分类信息'
path_save = r'H:\database_local\stock\temp_predict_indexmem\sample_industry'


def filter_gfcy(term):
    index_name = '光伏产业'
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    #
    data1 = pd.read_excel(os.path.join(path_folder, '光伏.xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, '半导体硅片.xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, '光伏玻璃.xlsx'))['证券代码']
    data4 = pd.read_excel(os.path.join(path_folder, 'SW光伏电池组件.xlsx'))['证券代码']
    data5 = pd.read_excel(os.path.join(path_folder, '光伏逆变器.xlsx'))['证券代码']
    data6 = pd.read_excel(os.path.join(path_folder, '光伏电站.xlsx'))['证券代码']
    code = pd.concat([data1, data2, data3, data4, data5, data6]).drop_duplicates()
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), len(data3), len(data4), len(data5), len(data6), '\n池中样本总数：', len(code), '，属于上期样本的池中样本：', len(code_in_old)) # 50 98 49
    return code


def filter_5Gtx(term):
    index_name = '5G通信'
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    #
    data1 = pd.read_excel(os.path.join(path_folder, '5G.xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, '5G应用.xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, 'PC 5G.xlsx'))['证券代码']
    code = pd.concat([data1, data2, data3]).drop_duplicates()
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), len(data3), '\n池中样本总数：', len(code), '，属于上期样本的池中样本：', len(code_in_old)) # 50 293 47
    return code


def filter_CSxnc(term):
    index_name = 'CS新能车'
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    #
    data1 = pd.read_excel(os.path.join(path_folder, '锂电池.xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, '充电桩.xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, '新能源整车.xlsx'))['证券代码']
    data4 = pd.read_excel(os.path.join(path_folder, '新能源汽车.xlsx'))['证券代码']
    data5 = pd.read_excel(os.path.join(path_folder, '新能源.xlsx'))['证券代码']
    # data6 = pd.read_excel(os.path.join(path_folder, '光伏电站.xlsx'))['证券代码']
    code = pd.concat([data1, data2, data3, data4, data5]).drop_duplicates()
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), len(data3), len(data4), len(data5), '\n池中样本总数：', len(code), '，属于上期样本的池中样本：', len(code_in_old)) # 50 205 46
    return code


def filter_xnyc(term):
    index_name = '新能源车'
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    #
    data1 = pd.read_excel(os.path.join(path_folder, '新能源整车.xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, '新能源汽车.xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, '充电桩.xlsx'))['证券代码']
    data4 = pd.read_excel(os.path.join(path_folder, 'CJSC锂电设备.xlsx'))['证券代码']
    data5 = pd.read_excel(os.path.join(path_folder, 'CS锂电设备.xlsx'))['证券代码']
    data6 = pd.read_excel(os.path.join(path_folder, '锂电设备(长江)成份.xlsx'))['证券代码']
    data7 = pd.read_excel(os.path.join(path_folder, '锂电设备(中信)成份.xlsx'))['证券代码']
    data8 = pd.read_excel(os.path.join(path_folder, 'CJSC电池材料.xlsx'))['证券代码']
    data9 = pd.read_excel(os.path.join(path_folder, '锂电池.xlsx'))['证券代码']
    code = pd.concat([data1, data2, data3, data4, data5, data6, data7, data8, data9]).drop_duplicates()
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), len(data3), '\n池中样本总数：', len(code), '，属于上期样本的池中样本：', len(code_in_old)) # 59 89 34
    main_busi = load_main_business()
    code_main_busi = main_busi[main_busi['code'].isin(code)]
    return code


def filter_zzyl(term):
    index_name = '中证医疗'
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    #
    data0 = load_wind_industry()
    data0 = data0[data0['ind_wind1'] == '医疗保健']
    data0 = data0['code']
    #
    data1 = pd.read_excel(os.path.join(path_folder, '医疗器械.xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, '医疗服务.xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, '医疗信息化(长江)成份.xlsx'))['证券代码']
    data4 = pd.read_excel(os.path.join(path_folder, 'CJSC医疗信息化.xlsx'))['证券代码']
    data5 = pd.read_excel(os.path.join(path_folder, '医疗50成份.xlsx'))['证券代码']
    code = pd.concat([data1, data2, data3, data4, data5, data0]).drop_duplicates()
    #
    path_bcinfo = r'H:\database_local\stock\temp_predict_indexmem\板块分类信息\公司补充信息.xlsx'
    bcinfo = pd.read_excel(path_bcinfo)
    bcinfo.rename(columns={'证券代码↑': 'code', '证券简称': 'name'}, inplace=True)
    new_space = bcinfo[bcinfo['code'].isin(code)]
    last_keywords = bcinfo[bcinfo['code'].isin(member_last)]['主营产品类型']
    last_keywords = last_keywords.apply(func=lambda x: x.split('、'))
    last_keywords = list(set(last_keywords.sum()))
    new_space_new = new_space[new_space['主营产品类型'].apply(func=lambda x: any(a in last_keywords for a in x.split('、')) if isinstance(x, str) else False)]
    print(len(new_space), len(new_space_new))
    code = new_space_new['code']
    #
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), len(data3), len(data4), len(data5), '\n池中样本总数：', len(code), '，属于上期样本的池中样本：', len(code_in_old)) # 49 92 45
    main_busi = load_main_business()
    code_main_busi = main_busi[main_busi['code'].isin(code)]
    return code


def filter_zzjg(term):
    index_name = '中证军工'
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    #
    data1 = pd.read_excel(os.path.join(path_folder, 'CS国防军工.xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, 'SW国防军工.xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, 'CJSC国防军工.xlsx'))['证券代码']
    code = pd.concat([data1, data2, data3]).drop_duplicates()
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), '\n池中样本总数：', len(code), '，属于上期样本的池中样本：', len(code_in_old)) # 64 123 59
    main_busi = load_main_business()
    code_main_busi = main_busi[main_busi['code'].isin(code)]
    return code


def filter_zhbdtxp(term):
    member_last = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\中华半导体芯片成份(上期).xlsx')['code']
    print('上期样本数：', len(member_last))
    #
    data1 = pd.read_excel(os.path.join(path_folder, '半导体材料.xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, '半导体设备.xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, '半导体封测.xlsx'))['证券代码']
    data4 = pd.read_excel(os.path.join(path_folder, '半导体硅片.xlsx'))['证券代码']
    data5 = pd.read_excel(os.path.join(path_folder, '半导体产业.xlsx'))['证券代码']
    data6 = pd.read_excel(os.path.join(path_folder, '半导体.xlsx'))['证券代码']
    data7 = pd.read_excel(os.path.join(path_folder, '芯片.xlsx'))['证券代码']
    code = pd.concat([data1, data2, data3, data4, data5, data6, data7]).drop_duplicates()
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), len(data3), len(data4), len(data5), len(data6), len(data7), '\n池中样本总数：', len(code), '，属于上期样本的池中样本：', len(code_in_old)) # 50 135 50
    return code


def filter_kccy50(term):
    index_name = '科创创业50'
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    #
    code_all = pd.read_excel(os.path.join(path_folder, '双创50用2.xlsx'))
    code_all.rename(columns={'证券代码': 'code', '证券简称': 'name', '所属战略性新兴产业分类\n[交易日期] 最新\n[行业级别] 一级行业↓': 'cate_lv1', '所属战略性新兴产业分类\n[交易日期] 最新\n[行业级别] 二级行业': 'cate_lv2', '所属战略性新兴产业分类\n[交易日期] 最新\n[行业级别] 三级行业': 'cate_lv3', '所属战略性新兴产业分类\n[交易日期] 最新\n[行业级别] 全部明细': 'cate_detail'}, inplace=True)
    code_all.dropna(subset=['cate_lv1'], inplace=True)
    cate_lv1 = code_all['cate_lv1'].drop_duplicates()
    cate_lv2 = code_all['cate_lv2'].drop_duplicates()
    cate_lv3 = code_all['cate_lv3'].drop_duplicates()
    cate_detail = code_all['cate_detail'].drop_duplicates()
    code_in_old = code_all[code_all['code'].isin(member_last)]
    code_not_in = member_last[~member_last.isin(code_all['code'])]
    print('池中样本总数：', len(code_all), '，属于上期样本的池中样本：', len(code_in_old))  # 50 1423 46
    #
    data1 = pd.read_excel(os.path.join(path_folder, '新一代信息技术产业(科创板).xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, '高端装备制造产业(科创板).xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, '新材料产业(科创板).xlsx'))['证券代码']
    data4 = pd.read_excel(os.path.join(path_folder, '新能源汽车产业(科创板).xlsx'))['证券代码']
    data5 = pd.read_excel(os.path.join(path_folder, '生物产业(科创板).xlsx'))['证券代码']
    data6 = pd.read_excel(os.path.join(path_folder, '新能源产业(科创板).xlsx'))['证券代码']
    data7 = pd.read_excel(os.path.join(path_folder, '节能环保产业(科创板).xlsx'))['证券代码']
    data8 = pd.read_excel(os.path.join(path_folder, '数字创意产业(科创板).xlsx'))['证券代码']
    # data11 = pd.read_excel(os.path.join(path_folder, '新一代信息技术产业.xlsx'))['证券代码']
    data22 = pd.read_excel(os.path.join(path_folder, '高端装备制造.xlsx'))['证券代码']
    data33 = pd.read_excel(os.path.join(path_folder, '新材料.xlsx'))['证券代码']
    data44 = pd.read_excel(os.path.join(path_folder, '新能源汽车.xlsx'))['证券代码']
    # data55 = pd.read_excel(os.path.join(path_folder, '生物产业.xlsx'))['证券代码']
    data66 = pd.read_excel(os.path.join(path_folder, '新能源.xlsx'))['证券代码']
    data77 = pd.read_excel(os.path.join(path_folder, '节能环保.xlsx'))['证券代码']
    datan1 = pd.read_excel(os.path.join(path_folder, '生物50成份.xlsx'))['证券代码']
    datan2 = pd.read_excel(os.path.join(path_folder, '信息技术(长江)成份.xlsx'))['证券代码']
    datan3 = pd.read_excel(os.path.join(path_folder, 'CN信息技术.xlsx'))['证券代码']
    datan4 = pd.read_excel(os.path.join(path_folder, '生物医药2(中信)成份.xlsx'))['证券代码']
    code = pd.concat([data1, data2, data3, data4, data5, data6, data7, data8, data22, data33, data44, data66, data77, datan1, datan2, datan3, datan4]).drop_duplicates()
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), len(data3), len(data4), len(data5), len(data6), len(data7), len(data8), len(data22), len(data33), len(data44), len(data66), len(data77), len(datan1), len(datan2), len(datan3), len(datan4), '\n池中样本总数：', len(code), '，属于上期样本的池中样本：', len(code_in_old)) # 50 1423 46
    return code


def filter_gzxp(term):
    index_name = '国证芯片'
    member_last = pd.read_excel(r'H:\database_local\stock\temp_predict_indexmem\国证芯片成份(上期).xlsx')['code']
    print('上期样本数：', len(member_last))
    #
    data1 = pd.read_excel(os.path.join(path_folder, '半导体材料.xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, '半导体设备.xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, '半导体封测.xlsx'))['证券代码']
    data4 = pd.read_excel(os.path.join(path_folder, '半导体硅片.xlsx'))['证券代码']
    data5 = pd.read_excel(os.path.join(path_folder, '半导体产业.xlsx'))['证券代码']
    data6 = pd.read_excel(os.path.join(path_folder, '半导体.xlsx'))['证券代码']
    data7 = pd.read_excel(os.path.join(path_folder, '芯片.xlsx'))['证券代码']
    # industry_zz = load_industry_cate_zz()
    # code_aim = industry_zz[industry_zz['industry_lv2'].isin(['半导体'])]['code']
    code = pd.concat([data1, data2, data3, data4, data5, data6, data7]).drop_duplicates()
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), len(data3), len(data4), len(data5), len(data6), len(data7), '\n池中样本总数：', len(code),
          '，属于上期样本的池中样本：', len(code_in_old))  # 30 135 30
    return code


def filter_swyy(term):
    index_name = '生物医药'
    index_name_df = load_index_list()
    index_code = index_name_df['code'].to_list()[index_name_df['name'].to_list().index(index_name)]
    index_member = load_aindexmembers(index_code)
    member_last = check_index(index_member, term)
    print('上期样本数：', len(member_last))
    #
    data1 = pd.read_excel(os.path.join(path_folder, '生物医药2(中信)成份.xlsx'))['证券代码']
    data2 = pd.read_excel(os.path.join(path_folder, '生物医药产业(科创板).xlsx'))['证券代码']
    data3 = pd.read_excel(os.path.join(path_folder, '生物产业(万德)指数成份.xlsx'))['证券代码']
    data_industry = load_industry_cate_zz()
    data_industry = data_industry[data_industry['industry_lv3'].isin(['医疗器械', '制药与生物科技服务', '生物药品'])]
    code = pd.concat([data1, data2, data3, data_industry['code']]).drop_duplicates()
    code_in_old = code[code.isin(member_last)]
    print(len(data1), len(data2), len(data3), len(data_industry), '\n池中样本总数：', len(code),
          '，属于上期样本的池中样本：', len(code_in_old)) # 30 217 29
    return code


if __name__ == '__main__':
    # filter_gfcy('2022-06-30') # 50 98 49
    # filter_5Gtx('2022-06-30') # 50 293 47
    # filter_CSxnc('2022-06-30') # 50 205 46
    # filter_xnyc('2022-06-30') # 这个暂时还不能用 59 89 34，因为在制定业务范围内的全部证券需要入选
    filter_zzyl('2022-06-30') # 49 92 45
    # filter_zzjg('2022-06-30') # 64 123 59
    # filter_zhbdtxp('2022-06-30') # 50 135 50 至今最完美的
    filter_kccy50('2022-06-30')  # 50 1423 46 很努力了
    # filter_gzxp('2022-06-30') # 30 135 30
    # filter_swyy('2022-06-30') # # 30 217 29
