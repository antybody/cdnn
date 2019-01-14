
import datetime
import operator

from sqlalchemy import create_engine

from cdnn.pyculiarity import detect_ts
from  cdnn.models.median import detectoutliers
from  cdnn.models.arima import arima_run
from  cdnn.models.smooth import moving_average,fast_moving_average
from  cdnn.models.clof import clof

from cdnn.predata.pre_lof import pre_lof, replace_zero, replace_data_lg, replace_data, replace_data_lg_max
from  cdnn.predata.pre_knn import pre_knn

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 数据库相关
from cdnn.db.main import select

import impyute as impy

from cdnn.utils.publicUtil import get_mode, oracleUtil

sheetname = ['86-2','12021-2','37480-2','18527_2','19573_3']


def get_data():
    twitter_example_data = pd.read_excel('D:\workroot\workspace\pythonpace1\python\cdnn\data\ele.xlsx',sheet_name=sheetname[2])
    # 这次是处理后的数据
    data_after = pre_lof(twitter_example_data, '', '')
    # data_after = pre_lof(twitter_example_data, '2017', '')
    # data_after = pre_lof(twitter_example_data,'2017-10','')
    # data_after = pre_lof(twitter_example_data,'2017-10-05','2017-10-19')
    # data_after.reset_index(drop=False, inplace=True)
    return twitter_example_data

def get_db_data():

    starttime = '2017-01-01'
    endtime = '2017-12-30'

    #数据sql
    sql = "select id,dt_time,dt_val from error_in where 1=1 and to_date(dt_time,'yyyy/mm/dd') >=  to_date('"  +starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" +endtime+"','yyyy/mm/dd')"
    data = select(sql)

    #泵站名字集合
    fields_sql =  "select id from error_in where 1=1 and to_date(dt_time,'yyyy/mm/dd') >=  to_date('"  +starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" +endtime+"','yyyy/mm/dd') group by id"
    fields=select(sql)

    # 处理成python 可识别的
    df = pd.DataFrame(list(data), columns=['id','dt_time','dt_val'])

    print(df)
    return df

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


''' 四分法 测试 12'''

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

def create_arima(field,data):
    # 显示所有列
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)

    data[['dt_val']]=data[['dt_val']].astype(float)
    print(data)
    plt.figure()
    data['dt_val'].plot()
    df = arima_run(data)
    obj=np.array(df).tolist();
    print("差值数据")
    print(df)
    print("obj是",len(obj),"df",len(df))
    model = get_mode(obj)
    print("众数",model[0])
    it=pd.DataFrame({'key': obj })

    it_copy=it.copy()

    # 删除/选取某列含有特定数值的行
    # 通过~取反，选取不包含数字model的行
    it=it[~it['key'].isin(model)]


    print("剔除mode后")


    data_index = [];#记录所有异常点的坐标位置,在元数据中

    #1.找出0位置
    data_zero=data[(data.dt_val == 0)].dt_time.tolist()


    # 2.找出负值位置
    data_minus=data[(data.dt_val < 0)].dt_time.tolist()


    #3.缺失数据
    # 补充了数据 ,这时补充的数据都是 0
    data2 = data.copy()
    data2['dt_time']=data2['dt_time'].astype('datetime64')
    df_period = data2.resample('D', on='dt_time').sum()
    df_period=df_period.reset_index()
    data_miss = []
    # 求差集
    a=df_period.copy();
    b = data.copy()
    b['dt_time']=b['dt_time'].astype('datetime64')
    miss_result=a.append(b).drop_duplicates(subset=['dt_time'], keep=False)
    data_miss=miss_result.dt_time.tolist()



    # 判断没有model的情况  证明没有异常点
    if not (operator.eq(model, obj)):
        print("it处理数据",it)
        end = it.tail(1)["key"].index.values[0]#最后一个不做处理

        # a=it.iloc[len(it), 1]
        print("剔除mode后,长度为",len(it_copy),",剔除mode后,长度为", len(it),it.tail(1)["key"])

        it1=it_copy[it_copy['key'].isin(model)]
        print("model的个数为:",len(it1))

        # 寻找异常点
        for row in it.itertuples(index=True, name='Pandas'):
            if(row[1]<0 and row[0]>0 and row[0]<end) :
                        #当前点与model的差值
                        now = dist(row[1],model[0]);
                        #前一个点与model的差值
                        last=dist(it_copy.loc[[row[0] - 1]].values,model[0]);

                        if (round(it_copy.loc[row[0] - 1]["key"], 3)!= round(model[0], 3) and last>0 and abs(last) > 3 * abs(model[0])):
                            print("前一个点信息:",row[0] - 1, it_copy.loc[[row[0] - 1]].key, "是异常点")
                            data_index.append(row[0] - 1)
                            continue

                        # 后一个点与model的差值
                        next=dist(it_copy.loc[[row[0] + 1]].values,model[0]);
                        print(it_copy.loc[row[0] + 1]["key"])
                        if (round(it_copy.loc[row[0] + 1]["key"], 3)!= round(model[0], 3) and next > 0 and abs(next) > 3 * abs(model[0])):
                            print("当前点信息:", row[0] , it.loc[[row[0]]]["key"], "是异常点")
                            data_index.append(row[0] )
                            continue

                        #同为负值的情况
                        # if (row[0]>1 and round(it_copy.loc[row[0] -2]["key"], 3)!= round(model[0], 3) and round(it_copy.loc[row[0] -2]["key"], 3)<0 and now < 0 ):
                        #     print("当前点信息:", row[0]-2 , it.loc[[row[0]-2]]["key"], "后置异常点")
                        #     data_index.append(row[0]-2 )

    data_index=[i + 1 for i in data_index]#修正为元数据中的位置
    data_error=[]#异常点集合

    data_allindex = data.index.tolist()
    for i, val in enumerate(data_index):
        print("序号：%s   值：%s" % (i + 1, val))
        if i in data_allindex:
            data_error.append(data.loc[[val]].dt_time[val])


   #极值
    Elist = data_zero
    if data_minus :
        Elist=Elist+data_minus
    if data_miss:
        Elist=Elist+data_miss
    data_max=list(set(data_error).difference(set(Elist)))
    #无异常数列
    if data_max:
        Elist=Elist+data_max

    #构标准时间段
    data_normal=data[~data['dt_time'].isin(Elist)]

    data_amend = replace_zero(df_period.copy())#负值替换成0
    data_amend =replace_data_lg(data_amend.copy())#0替换成修正值
    data_amend=data_amend.reset_index()

    #处理结果封装成标准字段
    datas_normal=result(data_normal.dt_time.tolist(), data, data_amend, 0)
    datas_zero=result(data_zero, data, data_amend, 2)
    datas_miss = result(data_miss, data, data_amend, 1)#缺失值
    datas_minus=result(data_minus, data, data_amend, 3)
    #极值处理
    datas_max=result(data_max, data, data_amend, 4)
    if not datas_max.empty:
        datas_max.dt_eidt=[0 for i in range(len(datas_max))]
        datas_max=replace_data_lg_max(datas_max.copy())  # 0替换成修正值
        # datas_max=datas_max.reset_index()

   #汇总所有数据
    list_all=pd.concat([datas_normal, datas_zero, datas_miss, datas_minus, datas_max], axis=0,
                               ignore_index=True)
    list_all['id']=[field for i in range(len(list_all))]  # 添加id列

    list_all = list_all.sort_values(by = 'dt_time',axis = 0,ascending = True)#按时间排序
    list_all.reset_index()
    list_all['dt_time']=list_all['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))

    #类型转换
    list_all=list_all.astype('str')
    data=data.astype('object')
    oracleUtil("gxsy:gxsy123@120.26.116.232:1521/orcl", data, 'data_in')
    oracleUtil("gxsy:gxsy123@120.26.116.232:1521/orcl",list_all, 'error_out')
    print(list_all)
    # df.plot()
    # plt.show()

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

    # 判断当前值与model的相对位置
def dist(num, model):
    result=num - model
    return result

    '''
    #把list整理成标准json   data为修正后的数据
    list:要处理的时间戳
    old_data:基准数据
    new_data:对比数据
    type:异常类型
    '''
def result(list, old_data, new_data, type):
    if not list:
        return pd.DataFrame()
    result=[]
    for i, values in enumerate(list):
        dt_item=new_data[(new_data.dt_time == str(values))].copy()  # 获取一行数据

        dt_item["dt_reason"]=type  # 添加异常类型
        dt_item["dt_eidt"]=dt_item["dt_val"].values  # 修正值
        if type != 1:  # 如果不是缺失数据就填充其原始数据
            dt_item_old=old_data[(old_data.dt_time == str(values))].copy()  # 获取一行数据传入原数据
            dt_item["dt_val"]=dt_item_old["dt_val"].values  # 老数据中的val
        else:
            dt_item["dt_val"]=''  # 老数据中的val
        # else:
        #     dt_item["dt_eidt"]=dt_item["dt_val"].values
        if i == 0:
            result=dt_item.copy()
            continue
        result=result.append(dt_item, ignore_index=True)
    return result


def data_list(starttime,endtime):
    # 数据sql
    sql="select id,dt_time,dt_val from error_in where 1=1 and to_date(dt_time,'yyyy/mm/dd') >=  to_date('" + starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" + endtime + "','yyyy/mm/dd')"
    datas=select(sql)
    # 泵站名字集合
    fields_sql="select id from error_in where 1=1 and to_date(dt_time,'yyyy/mm/dd') >=  to_date('" + starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" + endtime + "','yyyy/mm/dd') group by id"
    fields=select(fields_sql)

    # 处理成python 可识别的
    df=pd.DataFrame(list(datas), columns=['id', 'dt_time', 'dt_val'])
    for i in range(len(fields)):
        val =fields[i][0]
        print(val)
        data=df[(df.id ==fields[i][0])]
        data.index=[i for i in range(len(data))]
        create_arima(val, data)




if __name__ ==  '__main__':
    starttime='2017-01-01'
    endtime='2017-12-30'
    data_list(starttime,endtime)




