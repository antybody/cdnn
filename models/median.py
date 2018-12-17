#-*-coding:utf-8-*-
"""
四分位法 通过训练样本 计算 最大最小值
用该数据 判断 数据
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt



def detectoutliers(data):

    dt = data['FP_TOTALENG']
    dt = (np.array(dt).tolist())
    outlier_list_title = []
    outlier_list_col = []

    # Q1 为数据占25% 的数据值范围
    Q1 = np.percentile(dt,25)
    print(Q1)

    #Q3 为数据占 75% 的数据范围
    Q3 = np.percentile(dt,75)
    print(Q3)

    IQR = Q3 - Q1

    # 异常值的范围
    outlier_step = 1.5* IQR
    print(outlier_step)

    # for n in range(len(dt)):
    #     if float(dt[n]) < Q1 - outlier_step or float(dt[n]) > Q3 + outlier_step:
    #         outlier_list_title.append(data[n:n+1]['TIMESTAMP'])
    #         outlier_list_col.append(data[n:n+1]['FP_TOTALENG'])
    dd = data[ (data['FP_TOTALENG'] < Q1 - outlier_step) | (data['FP_TOTALENG'] > Q3 + outlier_step)]

    return dd



# 从excel 中读取数据

ele_file = '../ele.xls' #餐饮数据

data = pd.read_excel(ele_file, sheet_name='37480-2') #读取数据


data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])


dt = data['FP_TOTALENG']


l = detectoutliers(data)

print(l)

plt.plot( data['TIMESTAMP'],dt, label=u'first')

# print(x,y)
plt.plot(l['TIMESTAMP'], l['FP_TOTALENG'], 'ro',label='check')

plt.legend()

plt.show()



