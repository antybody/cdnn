#-*-coding:utf-8-*-
'''
  填补数据
  根据模拟值 替换 0 值
  建议参考：https://blog.csdn.net/mhywoniu/article/details/78514664
  增加样条
'''
import pandas as pd
import numpy as np
import impyute as impy
import array

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
'''
  重新修改了，
  如果是大批量的异常的话，插补数据就出现问题了
  解决了该问题
'''
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
    dt_flag = cal_countines_dt(df_period_clone)
    dt_array_flag = [item for sublist in dt_flag for item in sublist]
    # print(dt_array_flag)
    # print(df_peroid_v.index)

    if isnullcon['dt_eidt']:
        for j in range(len(df_period_clone)):
            out = False
            kk = 0
            if np.isnan(df_period_clone['dt_eidt'][j]):
                for i in range(len(dt_flag)):
                    if j in dt_flag[i]:
                        out = True
                        kk = i
                        break
                if out :
                    df_period_clone['dt_eidt'][j] = lot_miss(df_period_clone,dt_flag[i],j)
                elif abs(j - inflex ) > 3 or inflex == 0:
                    df_period_clone['dt_eidt'][j] =    ploy_mean(df_period_clone,j) #ploy(df_peroid_v, j)
                else:
                    df_period_clone['dt_eidt'][j] = cal_inflexion_val(df_peroid_v_clone,j)

    return df_period_clone


# 查找大量连续缺失
def cal_countines_dt(dt):
    dt_flag = []
    dt_tmp = []
    dt_final = []
    k = 0
    for j in range(len(dt)):
        if np.isnan(dt['dt_eidt'][j]):
            dt_flag.append(j)

    for j in range(len(dt_flag)-1):
        if (dt_flag[j+1] - dt_flag[j]) ==1:
            dt_tmp.append(dt_flag[j])
            k = k +1
        else:
            if k > 3:
                k = 0
                dt_tmp.append(dt_tmp[-1]+1)
                dt_final.append(dt_tmp)
                dt_tmp = []
    return dt_final

# 替换处理如果是大量缺失的情况
def lot_miss(dt,dt_flag,j):
    if (dt_flag[0]) ==0:
        ft = dt['dt_eidt'][dt_flag[len(dt_flag)-1]+2]
    else:
        ft = dt['dt_eidt'][dt_flag[0]-1]

    if (dt_flag[-1] == len(dt)):
        ed = ft
    else:
        ed = dt['dt_eidt'][dt_flag[len(dt_flag)-1]+2]
    # 平均数
    dt_mean  = round((ed - ft)/len(dt_flag) ,2)
    if j == 0 :
        pre_dt = 0
    else:
        pre_dt = dt['dt_eidt'][j-1]
    if dt_mean >=0 :
        dt_final = round(pre_dt + dt_mean,2)
    else:
        dt_final = 0

    return dt_final

# 按平均值计算点
def ploy_mean(dt,j):

    if j == 0 :
        return dt['dt_eidt'][1]
    elif j == len(dt)-1:
        return dt['dt_eidt'][-2]
    else:
        ft = dt['dt_eidt'][j-1]
        ed = dt['dt_eidt'][j+3]
        # 平均数
        dt_mean  = round((ed - ft)/4 ,2)
        pre_dt = dt['dt_eidt'][j-1]

        dt_final = round(pre_dt + dt_mean,2)

        return dt_final

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


