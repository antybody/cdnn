#-*-coding:utf-8-*-

# 从excel 中读取数据

import pandas as pd

import matplotlib.pyplot as plt

from  cdnn.models.median import detectoutliers

'''
  以下是测试数据
'''

ele_file = '../data/eles.xlsx' # 电力数据

data = pd.read_excel(ele_file, sheet_name='994') #读取数据


data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])

# 数据排序
dt = data.sort_values( by='TIMESTAMP',ascending=True)

dt_992 = dt[dt['ID'] == 992]

# print(dt_992)

dt = dt_992['dt_val']


l = detectoutliers(dt_992)


plt.plot( dt_992['TIMESTAMP'],dt, label=u'first')

# print(x,y)
plt.plot(l['TIMESTAMP'], l['dt_val'], 'ro',label='check')

plt.legend()

plt.show()