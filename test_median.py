#-*-coding:utf-8-*-

# 从excel 中读取数据

import pandas as pd

import matplotlib.pyplot as plt

from  models.median import detectoutliers

'''
  以下是测试数据
'''

ele_file = 'ele.xls' # 电力数据

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