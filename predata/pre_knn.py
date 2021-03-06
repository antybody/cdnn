#-*-coding:utf-8-*-
'''
  填补数据
  根据模拟值 替换 0 值
  建议参考：https://blog.csdn.net/mhywoniu/article/details/78514664
'''
import pandas as pd
import numpy as np
import impyute as impy

def pre_knn(data,year,year1):
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
        df_after = impy.fast_knn(df_period_clone,k=13,eps=0,p=2)
        for i in range(len(df_period_clone)):
            # print('-----平缓后数据-----')
            # print(df_after[0][i])
            # print('-----平缓前数据-----')
            # print(df_period['FP_TOTALENG'][i])

            df_period_clone['FP_TOTALENG'][i] = df_after[0][i]

    return df_period_clone
