#-*-coding:utf-8-*-
'''
滑动平均
用来做异常数据处理
'''

import matplotlib.pyplot as plt
import pandas as pd
import requests
import io
import numpy as np


def moving_average(l, N):
    sum = 0
    result = list(0 for x in l)

    for i in range(0, N):
        sum = sum + l[i]
        result[i] = sum / (i + 1)

    for i in range(N, len(l)):
        sum = sum - l[i - N] + l[i]
        result[i] = sum / N

    return result


# 使用效率更高的numpy
# http://stackoverflow.com/questions/13728392/moving-average-or-running-mean
def fast_moving_average(x, N):
    return np.convolve(x, np.ones((N,)) / N)[(N - 1):]


# url = 'http://blog.topspeedsnail.com/wp-content/uploads/2016/12/铁路客运量.csv'
# ass_data = requests.get(url).content
#
# df = pd.read_csv(io.StringIO(ass_data.decode('utf-8')))  # python2使用StringIO.StringIO

# ele_file = '../data/ele.xls' # 电力数据
#
# df = pd.read_excel(ele_file, sheet_name='12021-2') #读取数据


