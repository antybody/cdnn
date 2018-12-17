#-*-coding:utf-8-*-
"""
四分位法 通过训练样本 计算 最大最小值
用该数据 判断 数据
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt



def detectoutliers(data):
    data = np.array(data).tolist()
    outlier_list_col = []

    # Q1 为数据占25% 的数据值范围
    Q1 = np.percentile(data,25)

    #Q3 为数据占 75% 的数据范围
    Q3 = np.percentile(data,75)

    IQR = Q3 - Q1

    # 异常值的范围
    outlier_step = 1.5* IQR
    for n in range(len(data)):
        if float(data[n]) < Q1 - outlier_step or float(data[n]) > Q3 + outlier_step:
            outlier_list_col.append(data[n])

    return outlier_list_col


dt = [6, 47, 49, 15, 42, 41, 7, 39, 43, 40, 36,180]


# print(np.array(dt))

# l = detectoutliers(dt)

# print(l)


