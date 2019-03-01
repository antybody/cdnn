#-*-coding:utf-8-*-

# 从excel 中读取数据

import pandas as pd

import matplotlib.pyplot as plt
import numpy as np

from luminol.anomaly_detector import  AnomalyDetector

from cdnn.pyculiarity import detect_ts
from  cdnn.models.median import detectoutliers

'''
  以下是测试数据
'''

ele_file = '../data/eles.xlsx' # 电力数据

data = pd.read_excel(ele_file, sheet_name='994') #读取数据


data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])

pd.set_option('display.max_rows', None)

# 数据排序
dt = data.sort_values( by='TIMESTAMP',ascending=True)

dt_992 = dt[dt['ID'] == 993]


dts = dt_992.copy()

dts.drop("ID", axis=1, inplace=True)

# print(dts)

# print(dts['TIMESTAMP'])

# dt = dts['DT_VAL']

dt = dts['dt_val']

dts.reset_index(drop=False,inplace=True)

dts.drop("index", axis=1, inplace=True)
# print(dts)
# for i in range(len(dts)):
#     # print(dts['dt_val'][0])
#     if dts['dt_val'][i] == 0:
#         dts['dt_val'][i] = dt.mean()

dts.replace(0, np.nan, inplace=True)

dtss = dts.dropna()

result = detect_ts(dtss,
                        max_anoms=0.05,
                        direction='both', e_value=True)
l = result['anoms']


plt.figure()
plt.plot( dtss['timestamp'],dtss['value'], color='g',label=u'first')

# plt.plot( dt_992['TIMESTAMP'],dts, label=u'second')

# print(x,y)
plt.plot(l['timestamp'], l['anoms'], 'ro',label='check')

plt.legend()

plt.show()