#-*-coding:utf-8-*-

import pandas as pd

import matplotlib.pyplot as plt

import numpy as np

import impyute as impy

from  cdnn.models.smooth import moving_average,fast_moving_average
from cdnn.predata.pre_lof import pre_lof, replace_zero, replace_data_lg, replace_data, replace_data_lg_max



ele_file = '../data/eles.xlsx' # 电力数据

data = pd.read_excel(ele_file, sheet_name='994') #读取数据


data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])

# 数据排序
dt = data.sort_values( by='TIMESTAMP',ascending=True)

dt = dt.set_index('TIMESTAMP')

dt_992 = dt[dt['ID'] == 992]

dt_992[['dt_val']]=dt_992[['dt_val']].astype(float)


dt_992.drop("ID", axis=1, inplace=True)

# df_period = dt_992.resample('D').sum()
#
# df_period.replace(0, np.nan, inplace=True)

# print(df_period['dt_val'])

ma_data = moving_average(np.array(dt_992['dt_val']).tolist(), 3)


# 平滑后的数据
# df_after = impy.locf(df_period,axis='dt_val')

pd.set_option('display.max_rows', None)
# print(df_after.astype(float))



plt.figure()
# plt.plot(dt_992, color='g')
plt.plot(ma_data, color='r')
plt.show()