
import datetime
import operator

from sqlalchemy import create_engine

from cdnn.pyculiarity import detect_ts
from  cdnn.models.median import detectoutliers
from  cdnn.models.arima import arima_run
from  cdnn.models.smooth import moving_average,fast_moving_average
from  cdnn.models.clof import clof

from cdnn.predata.pre_lof import pre_lof, replace_zero, replace_data_lg, replace_data, replace_data_lg_max, \
    find_inflexion
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

def create_arima(field,data):

    #原数据预处理
    data['dt_time']=data['dt_time'].astype('datetime64')
    data[['dt_val']]=data[['dt_val']].astype(float)
    data=data.sort_values(by='dt_time', axis=0, ascending=True)  # 按时间排序
    data['dt_time']=data['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))
    data.index=[i for i in range(len(data))]
    # 显示所有列
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    print(data)
    plt.figure()
    data['dt_val'].plot()

    model=None
    # 找到拐点,定义model
    inflexion=find_inflexion(data[(data.dt_val != 0)].dt_val)
    data_head=None
    data_foot=None
    df=arima_run(data)
    obj=np.array(df).tolist();
    if (inflexion != 0):
        data_head=data[0:inflexion]
        data_foot=data[inflexion:]
        model=get_modes(data_head)
        model=model+get_modes(data_foot)
        inflexion=inflexion-1
    else:
        print("差值数据")
        print(df)
        print("obj是", len(obj), "df", len(df))
        model=get_mode(obj)
        print("众数", model[0])

    it=pd.DataFrame({'key': obj })
    it_copy=it.copy()

    # 删除/选取某列含有特定数值的行
    # 通过~取反，选取不包含数字model的行
    if (inflexion != 0):#多model不予剔除model
        it=it[~it['key'].isin(model)]
        print("剔除mode后")


    data_index = [];#记录所有异常点的坐标位置,在元数据中

    #1.找出0位置
    data_zero=data[(data.dt_val == 0)]
    # 2.找出负值位置
    data_minus=data[(data.dt_val < 0)]


    #3.缺失数据
    # 补充了数据 ,这时补充的数据都是 0
    data2 = data.copy()
    data2['dt_time']=data2['dt_time'].astype('datetime64')
    df_period = data2.resample('D', on='dt_time').sum()
    df_period=df_period.reset_index()
    data_miss = None
    # 求差集
    a=df_period.copy();
    b = data.copy()
    b['dt_time']=b['dt_time'].astype('datetime64')
    data_miss=a.append(b).drop_duplicates(subset=['dt_time'], keep=False)

    # 判断没有model的情况  证明没有异常点
    if not (operator.eq(model, obj)):
        print("it处理数据",it)
        end = it.tail(1)["key"].index.values[0]#最后一个不做处理

            # a=it.iloc[len(it), 1]
        print("剔除mode后,长度为", len(it_copy), ",剔除mode后,长度为", len(it), it.tail(1)["key"])

        it1=it_copy[it_copy['key'].isin(model)]
        print("model的个数为:", len(it1))

        if field=='37480_2':
            a=1

        # 寻找异常点
        for row in it.itertuples(index=True, name='Pandas'):

            if(row[1]<0 and row[0]>0 and row[0]<end and data.loc[row[0]+1].dt_val!=0) :

                        # module对比值
                        module=round(model[0], 3)
                        # 动态选取model 有拐点情况
                        if (inflexion != 0 and row[0]==inflexion):
                            module=round(model[1], 3)

                        #当前点与model的差值
                        now = dist(row[1],module);

                        # 同为负值的情况
                        if (row[0] > 1 and round(it_copy.loc[row[0] - 1]["key"], 3) < 0):
                            print("前一点是异常点(双负):", row[0] - 1, it.loc[[row[0] - 1]]["key"])
                            data_index.append(row[0] - 1)
                            continue
                        #前一个点与model的差值
                        last=dist(it_copy.loc[[row[0] - 1]].values,module);

                        if (round(it_copy.loc[row[0] - 1]["key"], 3)!= module and last>0 ):
                            print("前一个点信息:",row[0] - 1, it_copy.loc[[row[0] - 1]].key, "是异常点")
                            data_index.append(row[0] - 1)
                            continue

                        # 后一个点与model的差值
                        next=dist(it_copy.loc[[row[0] + 1]].values,module);
                        print(it_copy.loc[row[0] + 1]["key"])
                        if (round(it_copy.loc[row[0] + 1]["key"], 3)!= module and next > 0):
                            print("当前点信息:", row[0] , it.loc[[row[0]]]["key"], "是异常点")
                            data_index.append(row[0] )
                            continue

    if len(data_index)==1:
        data_index.clear()

    data_error=pd.DataFrame()
    if len(data_index) > 0:
        if(field=='18527_2'):
            a=1
        data_index=[i + 1 for i in data_index]#修正为元数据中的位置
        er_item = pd.DataFrame()
        for j in range(len(data_index)):
            data_error=data_error.append(data[(data.index == data_index[j])], ignore_index=True)

    # 极值
    data_max=pd.DataFrame()
    if data_error.empty:
        data_error_item = data_error.copy()
        data_error_item = data_error_item.append(data_zero)
        data_error_item=data_error_item.append(data_minus)
        # data_error_item=data_error_item.append(data_miss)
        data_max=data_error_item.drop_duplicates(subset=['dt_time'], keep=False)


    #无异常部分
    data_normal = pd.DataFrame()
    data_item=pd.concat([ data_zero, data_miss, data_minus, data_max], axis=0,
                       ignore_index=True)
    if data_error.empty:
        data_normal=df_period.copy().append(data_item).drop_duplicates(subset=['dt_time'], keep=False)


#二次过滤异常值
    bj=None
    if not data_max.empty:
        # 异常二次寻找
        bj=error_filter(data_normal, data_max)
        if not bj.empty:
            # 去掉异常数据
            datas_normal=data_normal.append(bj).drop_duplicates(subset=['dt_time'], keep=False)
            data_max=data_max.append(bj, ignore_index=True)

    #处理结果封装成标准字段
    datas_normal= result(data_normal, 0)
    datas_zero=result(data_zero, 2)
    datas_miss = result(data_miss, 1)#缺失值
    datas_minus=result(data_minus, 3)
    #极值处理
    datas_max=result(data_max, 4)


    #异常填充成0
    error_item=pd.concat([data_zero, data_miss, data_minus, data_max], axis=0,
                        ignore_index=True)
    if error_item.empty:
        for j in range(len(error_item)):
            if not np.isnan(error_item['dt_val'][j]):
                error_item['dt_eidt'][j]=0

   #汇总所有数据
    list_all=pd.concat([datas_normal, datas_zero, datas_miss, datas_minus, datas_max], axis=0,
                               ignore_index=True)
    list_all['id']=[field for i in range(len(list_all))]  # 添加id列

    # list_all = list_all.sort_values(by = 'dt_time',axis = 0,ascending = True)#按时间排序
    list_all.reset_index()
    # list_all['dt_time']=list_all['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))


    #统一修正错误值
    for j in range(len(list_all)):
        if not np.isnan(list_all['dt_val'][j]):
            list_all['dt_eidt'][j]=list_all['dt_val'][j]
    list_all=replace_data_lg(list_all.copy())
    #类型转换
    list_all=list_all.astype('str')
    data=data.astype('object')
    oracleUtil("gxsy:gxsy123@120.26.116.232:1521/orcl", data, 'data_in')
    oracleUtil("gxsy:gxsy123@120.26.116.232:1521/orcl",list_all, 'data_out')
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


def get_modes(l):
    df=arima_run(l)
    obj=np.array(df).tolist();
    model=get_mode(obj)
    return model

    '''
    #把list整理成标准json   data为修正后的数据
    list:要处理的时间戳
    old_data:基准数据
    new_data:对比数据
    type:异常类型
    '''

def result(new_data, type):
    if new_data.empty:
        return pd.DataFrame()
    new_data["dt_reason"]=[type for i in range(len(new_data))]
    if type != 0:  # 如果不是正常数据就填充0
        new_data["dt_eidt"]=[0 for i in range(len(new_data))]
    else:
        new_data["dt_eidt"]=[np.nan for i in range(len(new_data))]
    return new_data



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
        create_arima(val,data)



def error_filter(data,exp):
    result=pd.DataFrame()
    if  not exp.empty :
        # 过滤异常集
        df_groupby=exp[['dt_val']].groupby(by='dt_val', as_index=False).max()
        df_merge=pd.merge(df_groupby, exp, on=[ 'dt_val'], how='left')
        result=data[(data.dt_val >= df_merge.loc[0].dt_val) & (data.dt_val <=df_merge.iloc[-1].dt_val)]
    return result



if __name__ ==  '__main__':
    starttime='2017-01-01'
    endtime='2017-12-30'
    data_list(starttime,endtime)




