#-*-coding:utf-8-*-
'''
  填补数据
  根据模拟值 替换 0 值
  建议参考：https://blog.csdn.net/mhywoniu/article/details/78514664
'''
import pandas as pd
import numpy as np
import impyute as impy

from scipy import interpolate
from scipy.interpolate import lagrange

def pre_lof(data,year,year1):
    # 设定时间序列
    # data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])
    data = data.set_index('TIMESTAMP')
    if  year.strip():
        if year1.strip():
            data = data[year:year1]
        else:
            data = data[year]
    # 补充了数据 ,这时补充的数据都是 0
    df_period = data.resample('D').sum()

    # 替换空值为其他值
    df_period_clone = df_period
    df_period_clone.replace(0, np.nan, inplace=True)

    # 平滑后的数据
    isnullcon = df_period_clone.isnull().any()
    # print(isnullcon['FP_TOTALENG'])
    # 这里来个判断，如果数据里没有需要平滑的点，那么直接输出
    if isnullcon['FP_TOTALENG']:
        df_after = impy.locf(df_period_clone, axis='FP_TOTALENG')
        for i in range(len(df_period_clone)):
            # print('-----平缓后数据-----')
            # print(df_after[0][i])
            # print('-----平缓前数据-----')
            # print(df_period['FP_TOTALENG'][i])

            df_period_clone['FP_TOTALENG'][i] = df_after[0][i]

    return df_period_clone


#将0值替换为平缓数据
def replace_data(df_period,field):
    pd.set_option('display.max_rows', None)
    # 替换空值为其他值
    df_period_clone = df_period
    df_period_clone.replace(0, np.nan, inplace=True)

    # 平滑后的数据
    isnullcon = df_period_clone.isnull().any()
    # print(isnullcon['FP_TOTALENG'])
    # 这里来个判断，如果数据里没有需要平滑的点，那么直接输出
    df_peroid_v = df_period_clone[field]
    df_peroid_v.reset_index(drop=True,inplace=True)
    print(df_peroid_v.index)
    if isnullcon[field]:
        df_after = impy.locf(df_period_clone, axis=field)
        for i in range(len(df_period_clone)):
            print('-----平缓后数据-----')
            print(df_after[0][i])
            print('-----平缓前数据-----')
            print(df_period[field][i])

            df_period_clone[field][i] = df_after[0][i]
    # print(df_period_clone)
    return df_period_clone

# 拉格朗日
def replace_data_lg(df_period):
    # pd.set_option('display.max_rows', None)
    # 替换空值为其他值
    df_period_clone = df_period
    df_period_clone.replace(0, np.nan, inplace=True)

    # 平滑后的数据
    isnullcon = df_period_clone.isnull().any()
    # print(isnullcon['FP_TOTALENG'])
    # 这里来个判断，如果数据里没有需要平滑的点，那么直接输出
    df_peroid_v = df_period_clone['dt_val']
    df_peroid_v.reset_index(drop=True, inplace=True)
    print(df_peroid_v.index)
    if isnullcon['dt_val']:
        for j in range(len(df_period_clone)):
            if np.isnan(df_period_clone['dt_val'][j]):
                df_period_clone['dt_val'][j] = ploy(df_peroid_v, j)

    return df_period_clone

# 拉格朗日

def ploy(s,n,k=4):
    y = s[list(range(n - k, n)) + list(range(n + 1, n + 1 + k))]  # 取数
    y = y[y.notnull()]
    out = lagrange(y.index, list(y))(n)
    return round(out,2)

#将指定列值替换为0
def replace_zero(data,field):
    # data = datas.copy()
    data=data.set_index('dt_time')
    for i in range(len(data)):
        aaa=data[field][i]
        if (data[field][i] < 0):
            print(data[field][i])
    return data


# 拉格朗日1
def replace_data_lg_max(df_period):
    # pd.set_option('display.max_rows', None)
    # 替换空值为其他值
    df_period_clone = df_period
    df_period_clone.replace(0, 0, inplace=True)

    # 平滑后的数据
    isnullcon = df_period_clone.isnull().any()
    # print(isnullcon['FP_TOTALENG'])
    # 这里来个判断，如果数据里没有需要平滑的点，那么直接输出
    df_peroid_v = df_period_clone['dt_eidt']
    df_peroid_v.reset_index(drop=True, inplace=True)
    print(df_peroid_v.index)
    if isnullcon['dt_eidt']:
        for j in range(len(df_period_clone)):
            if df_period_clone['dt_eidt'][j]==0:
                df_period_clone['dt_eidt'][j] = ploy(df_peroid_v, j)

    return df_period_clone

