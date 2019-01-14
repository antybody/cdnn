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
from scipy.interpolate import interp1d

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
def replace_data(df_period):
    pd.set_option('display.max_rows', None)
    # 替换空值为其他值
    df_period_clone = df_period
    df_period_clone.replace(0, np.nan, inplace=True)

    # 平滑后的数据
    isnullcon = df_period_clone.isnull().any()
    # print(isnullcon['FP_TOTALENG'])
    # 这里来个判断，如果数据里没有需要平滑的点，那么直接输出
    df_peroid_v = df_period_clone['dt_val']
    df_peroid_v.reset_index(drop=True,inplace=True)
    print(df_peroid_v.index)
    if isnullcon['dt_val']:
        df_after = impy.locf(df_period_clone, axis='dt_val')
        for i in range(len(df_period_clone)):
            print('-----平缓后数据-----')
            print(df_after[0][i])
            print('-----平缓前数据-----')
            print(df_period['dt_val'][i])

            df_period_clone['dt_val'][i] = df_after[0][i]
    # print(df_period_clone)
    return df_period_clone

# 拉格朗日
def replace_data_lg(df_period):
    # pd.set_option('display.max_rows', None)
    # 替换空值为其他值
    df_period_clone = df_period
    df_period_clone['dt_eidt'].replace(0, np.nan, inplace=True)

    # 平滑后的数据
    isnullcon = df_period_clone.isnull().any()
    # print(isnullcon['FP_TOTALENG'])
    # 这里来个判断，如果数据里没有需要平滑的点，那么直接输出
    df_peroid_v = df_period_clone['dt_eidt']
    df_peroid_v.reset_index(drop=True, inplace=True)
    df_peroid_v_clone = df_peroid_v.copy()
    inflex =  find_inflexion(df_peroid_v_clone)
    # print(df_peroid_v.index)
    if isnullcon['dt_eidt']:
        for j in range(len(df_period_clone)):
            if np.isnan(df_period_clone['dt_eidt'][j]):
                if abs(j - inflex ) > 3 or inflex == 0:
                    df_period_clone['dt_eidt'][j] = ploy(df_peroid_v, j)
                else:
                    df_period_clone['dt_eidt'][j] = cal_inflexion_val(df_peroid_v_clone,j)

    return df_period_clone

# 拉格朗日
def ploy(s,n,k=3):
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

# 样条插值
def replace_date_slin(df_period):
    # 替换空值为其他值
    df_period_clone = df_period
    df_period_clone.replace(0, np.nan, inplace=True)

    # 平滑后的数据
    isnullcon = df_period_clone.isnull().any()
    # print(isnullcon['FP_TOTALENG'])
    # 这里来个判断，如果数据里没有需要平滑的点，那么直接输出
    df_peroid_v = df_period_clone['dt_val']
    df_peroid_v.reset_index(drop=True, inplace=True)
    df_peroid_v.dropna(axis=0, how='any')
    data_allindex = df_peroid_v.index.tolist()
    fun = interp1d(data_allindex, df_peroid_v, kind= 'zero')
    if isnullcon['dt_val']:
        for j in range(len(df_period_clone)):
            if np.isnan(df_period_clone['dt_val'][j]):
                df_period_clone['dt_val'][j] = fun(j)

    return df_period_clone

# 找出拐点
'''
 1、 如果拐点所在的序号 < 4 那么就不算有拐点出现
 2、 获取拐点所在的序列号，循环 数据时判断 序号是否在其左右，如果在的话，那么就不采用拉格朗日计算方法 
 3、 根据 拐点的数值 减去 众值 作为 最后的结果
'''
def find_inflexion(df):
    df_clone = df.copy()
    # df_clone.replace(np.nan, 99999999, inplace=True)
    # 找到 拐点出现的序号
    inflexion_index = np.argmin(df_clone,axis=0)
    val = df_clone.min(axis=0)
    if inflexion_index < 4:
        return 0
    else:
        return inflexion_index

# 从拐点开始重新计算修正值
def cal_inflexion_val(df,j):
    df_clone = df.copy()
    # df_clone.replace(0, 99999999, inplace=True)
    # 获取拐点的值
    inflexion_val =  df_clone.min(axis=0)

    if j - find_inflexion(df) < 0 : # 说明 缺失值 在 拐点前面，需要从拐点开始递减
        return inflexion_val - 1 * abs(j - find_inflexion(df))
    else:
        return inflexion_val + 1 * abs(j - find_inflexion(df))


