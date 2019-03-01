#-*-coding:utf-8-*-
import pandas as pd

import matplotlib.pyplot as plt

from cdnn.pyculiarity import detect_ts
from  cdnn.models.median import detectoutliers

ele_file = '../data/eles.xlsx' # 电力数据

data = pd.read_excel(ele_file, sheet_name='994') #读取数据


data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])

# 数据排序
dt = data.sort_values( by='TIMESTAMP',ascending=True)

dt_992 = dt[dt['ID'] == 992]

dt = dt_992['dt_val']

# print(dt_992['TIMESTAMP'][1:])
D_data = dt_992['dt_val'].diff()

print(D_data)


plt.plot( dt_992['TIMESTAMP'],dt_992['dt_val'], label=u'first')

# print(x,y)
plt.plot(dt_992['TIMESTAMP'], D_data,label='check')

plt.legend()

plt.show()

