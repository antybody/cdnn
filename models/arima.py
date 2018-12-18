#-*-coding:utf-8-*-
'''
指数平滑
https://blog.csdn.net/u014096903/article/details/79980036
https://www.cnblogs.com/junge-mike/p/9335054.html
如果考虑利用差分的结果来查找毛刺数据
取昨天或者同比的数据分析 -- 当前的 跳跃阀值
'''

import matplotlib.pyplot as plt
import pandas as pd
import requests
import io
import numpy as np
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

ele_file = '../ele.xls' # 电力数据

df = pd.read_excel(ele_file, sheet_name='86-2') #读取数据

data = np.array(df['FP_TOTALENG'])

# 自相关性图
# plot_acf(data)
plt.figure()
df['FP_TOTALENG'].plot()
# plt.show()

# 平稳性检测
''' 这里输出的第二个指标P 如果大于 0.05 就是 非平稳序列 '''
from statsmodels.tsa.stattools import adfuller
# print('原始序列的检验结果为：',adfuller(data))

# 差分
# print(df['FP_TOTALENG'])
D_data = df['FP_TOTALENG'].diff().dropna()
# D_data.columns = [u'差分']
# print(D_data[72])
# print(df['FP_TOTALENG'])
D_data.plot()   #画出差分后的时序图
#
# plot_acf(D_data)
#
plt.show()

print(u'差分序列的ADF 检验结果为： ', adfuller(D_data))   #平稳性检验
#一阶差分后的序列的时序图在均值附近比较平稳的波动， 自相关性有很强的短期相关性， 单位根检验 p值小于 0.05 ，所以说一阶差分后的序列是平稳序列

from statsmodels.stats.diagnostic import acorr_ljungbox
print(u'差分序列的白噪声检验结果：',acorr_ljungbox(D_data, lags= 1)) #返回统计量和 p 值 p值小于 0.05

# 对模型定阶
# from statsmodels.tsa.arima_model import ARIMA
# pmax = int(len(D_data) / 10)    #一般阶数不超过 length /10
# qmax = int(len(D_data) / 10)
# bic_matrix = []
# for p in range(pmax +1):
#     temp= []
#     for q in range(qmax+1):
#         try:
#             temp.append(ARIMA(data, (p, 1, q)).fit().bic)
#         except:
#             temp.append(None)
#         bic_matrix.append(temp)
#
# bic_matrix = pd.DataFrame(bic_matrix)   #将其转换成Dataframe 数据结构
# p,q = bic_matrix.stack().idxmin()   #先使用stack 展平， 然后使用 idxmin 找出最小值的位置
# print(u'BIC 最小的p值 和 q 值：%s,%s' %(p,q))  #  BIC 最小的p值 和 q 值：0,1
# #所以可以建立ARIMA 模型，ARIMA(0,1,1)
# model = ARIMA(data, (p,1,q)).fit()
# model.summary2()        #生成一份模型报告
# model.forecast(5)   #为未来5天进行预测， 返回预测结果， 标准误差， 和置信区间
