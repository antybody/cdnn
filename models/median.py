#-*-coding:utf-8-*-
"""
四分位法 通过训练样本 计算 25% 中位数 75% 中位数，从而获取上下阈值
不适合线性数列
无参考
"""
import pandas as pd
import numpy as np



def detectoutliers(data):

    dt = data['dt_val']
    dt = (np.array(dt).tolist())
    outlier_list_title = []
    outlier_list_col = []

    # Q1 为数据占25% 的数据值范围
    Q1 = np.percentile(dt,25)

    #Q3 为数据占 75% 的数据范围
    Q3 = np.percentile(dt,75)

    IQR = Q3 - Q1
    # 异常值的范围
    outlier_step = 1.5* IQR

    # for n in range(len(dt)):
    #     if float(dt[n]) < Q1 - outlier_step or float(dt[n]) > Q3 + outlier_step:
    #         outlier_list_title.append(data[n:n+1]['TIMESTAMP'])
    #         outlier_list_col.append(data[n:n+1]['FP_TOTALENG'])
    dd = data[ (data['dt_val'] < Q1 - outlier_step) | (data['dt_val'] > Q3 + outlier_step)]

    return dd




