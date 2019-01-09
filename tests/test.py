
from cdnn.pyculiarity import detect_ts
from  cdnn.models.median import detectoutliers
from  cdnn.models.arima import arima_run
from  cdnn.models.smooth import moving_average,fast_moving_average
from  cdnn.models.clof import clof

from  cdnn.predata.pre_lof import pre_lof
from  cdnn.predata.pre_knn import pre_knn

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 数据库相关
from cdnn.db.main import select

import impyute as impy


sheetname = ['86-2','12021-2','37480-2','18527_2','19573_3']


def get_data():
    twitter_example_data = pd.read_excel('../data/ele.xlsx',sheet_name=sheetname[2])
    # 这次是处理后的数据
    # data_after = pre_lof(twitter_example_data, '', '')
    # data_after = pre_lof(twitter_example_data, '2017', '')
    # data_after = pre_lof(twitter_example_data,'2017-10','')
    # data_after = pre_lof(twitter_example_data,'2017-10-05','2017-10-19')
    # data_after.reset_index(drop=False, inplace=True)
    return twitter_example_data

def get_db_data():

    starttime = '2017-01-01'
    endtime = '2017-12-30'

    sql = "select id,dt_time,dt_val from error_in where 1=1 and to_date(dt_time,'yyyy/mm/dd') >=  to_date('"  +starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" +endtime+"','yyyy/mm/dd')"

    data = select(sql)

    # 处理成python 可识别的
    df = pd.DataFrame(list(data), columns=['id','dt_time','dt_val'])

    print(df)

'''时间序列的 S-H-ESD 算法测试 '''
''' 时间周期越短越准确'''
def create_pyculiarity():
    data = get_data()
    plt.figure()

    plt.plot(pd.to_datetime(data['TIMESTAMP']), data['FP_TOTALENG'], label=u'first')
    # 调用方法
    results = detect_ts(data,
                        max_anoms=0.3,
                        direction='both', e_value=True)
    # 输入检测结果
    print(results)
    x = pd.to_datetime(results['anoms']['timestamp'])
    plt.plot(x, results['anoms']['anoms'], 'ro', label='check')
    plt.legend()
    plt.show()


''' 四分法 测试'''

def create_median():
    data = get_data()
    data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])

    dt = data['FP_TOTALENG']

    l = detectoutliers(data)

    plt.title(u'median')
    plt.plot(data['TIMESTAMP'], dt, label=u'first')

    # print(x,y)
    plt.plot(l['TIMESTAMP'], l['FP_TOTALENG'], 'ro', label='check')

    plt.legend()

    plt.show()


'''差分法 测试'''

def create_arima():
    data = get_data()
    print(data)
    plt.figure()
    data['FP_TOTALENG'].plot()
    df = arima_run(data)
    print(df)
    df.plot()
    plt.show()

'''滑动平均 测试'''

def create_smooth():
    df = get_data()
    data = np.array(df['FP_TOTALENG'])

    ma_data = fast_moving_average(np.array(data).tolist(), 3)

    plt.figure()
    plt.plot(data, color='g')
    plt.plot(ma_data, color='r')
    plt.show()

'''聚类 测试'''

def create_clof():
    data = get_data()
    clof(data)


'''对数据预处理 测试'''
def pre_data():
    data = get_data()
    data_after = pre_lof(data, '', '')
    data_after.reset_index(drop=False,inplace=True)
    plt.figure()

    plt.plot(data['TIMESTAMP'], data['FP_TOTALENG'],label='first')
    plt.plot(data_after['TIMESTAMP'], data_after['FP_TOTALENG'],label='after')

    plt.legend()
    plt.show()

'''对数据预处理 测试'''
def pre_data_knn():
    data = get_data()
    data_after = pre_knn(data, '', '')
    data_after.reset_index(drop=False,inplace=True)
    plt.figure()

    plt.plot(data['TIMESTAMP'], data['FP_TOTALENG'],label='first')
    plt.plot(data_after['TIMESTAMP'], data_after['FP_TOTALENG'],label='after')

    plt.legend()
    plt.show()



if __name__ ==  '__main__':
    get_db_data()


