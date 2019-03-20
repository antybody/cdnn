
import datetime
import operator
import threading
import time

from sqlalchemy import create_engine
from cdnn.db.conns import getConfig
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
    twitter_example_data = pd.read_excel('D:\workroot\workspace\pythonpace1\python\cdnn\data\ele.xls',sheet_name=sheetname[0])
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
def create_pyculiarity(field,data):
    if len(data)<14:#过滤掉小于2周的数据
        return
    print(datetime.datetime.now())
    data['dt_time']=pd.to_datetime(data['dt_time'])
    data[['dt_val']]=data[['dt_val']].astype(float)
    data_copy=data.copy()
    data.drop("id", axis=1, inplace=True)
    # 调用方法
    results = detect_ts(data,
                        max_anoms=0.3,
                        direction='both', e_value=True)

    # 补充0后的数据 对比数据
    errors_result=get_errors(data_copy)
    df_period=errors_result[0]
    # 3.找出data缺失值
    data_miss=errors_result[1]
    datas_zero=pd.DataFrame()#0值异常
    datas_miss=pd.DataFrame()#缺失值异常
    datas_minus=pd.DataFrame()#负值异常
    datas_max=pd.DataFrame()#极值异常
    datas_normal=pd.DataFrame()#无异常集合
    list_all=pd.DataFrame()#处理后的结果集
    if not results['anoms'].empty:
        # 输入检测结果
        error_all = results['anoms'].copy()
        error_all.drop("expected_value", axis=1, inplace=True)
        error_all.rename(columns={'timestamp':'dt_time','anoms':'dt_val'}, inplace=True)

        # 处理异常数据l
        # 1.找出0位置
        data_zero=error_all[(error_all.dt_val == 0)]
        # 2.找出负值位置
        data_minus=error_all[(error_all.dt_val < 0)]

        # 4.极值
        data_max=error_all
        if not error_all.empty:
            a = error_all.copy()
            b=pd.concat([data_zero.copy(), data_minus.copy()], axis=0,
                                ignore_index=True)
            data_max=a.append(b).drop_duplicates(subset=['dt_time'], keep=False)

            # 5.无异常数列
            # 所有异常数据
            error_all=pd.concat([data_zero, data_miss, data_minus, data_max], axis=0,
                                ignore_index=True)

            data_normal=df_period.copy().append(error_all).drop_duplicates(subset=['dt_time'], keep=False)
            # 处理结果封装成标准字段
            datas_normal=result(data_normal.dt_time.tolist(), data_copy, df_period, 0)
            datas_zero=result(data_zero.dt_time.tolist(), data_copy, df_period, 2)
            datas_miss=result(data_miss.dt_time.tolist(), data_copy, df_period, 1)  # 缺失值
            datas_minus=result(data_minus.dt_time.tolist(), data_copy, df_period, 3)
            # 极值处理
            datas_max=result(data_max.dt_time.tolist(), data_copy, df_period, 4)
    else:
        datas_miss=result(data_miss.dt_time.tolist(), data_copy, df_period, 1)  # 缺失值
        data_normal=df_period.copy().append(datas_miss).drop_duplicates(subset=['dt_time'], keep=False)
        # 处理结果封装成标准字段
        datas_normal=result(data_normal.dt_time.tolist(), data_copy, df_period, 0)



    # 汇总所有数据
    list_all=pd.concat([datas_normal, datas_zero, datas_miss, datas_minus, datas_max], axis=0,
                               ignore_index=True)
    list_all['id']=field  # 添加id列
    list_all=list_all.sort_values(by='dt_time', axis=0, ascending=True)  # 按时间排序
    list_all.reset_index()
    list_all['dt_time']=list_all['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))

    list_all['dt_type']='pyculiarity'
    # 统一修正错误值
    list_all=replace_data_lg(list_all.copy())

    # 类型转换
    list_all=list_all.astype('str')
    data_copy=data_copy.astype('object')
    data_copy['dt_time']=data_copy['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))

    db= getConfig()
    oracleUtil(db['username'] + ':' + db['password'] + '@' + db['url'] + '/' + db['sid'], list_all, 'error_out')
    print(datetime.datetime.now())


''' 四分法 测试'''

def create_median(field,data):
    print(datetime.datetime.now())
    data_copy = data.copy()
    data['dt_time'] = pd.to_datetime(data['dt_time'])
    data[['dt_val']]=data[['dt_val']].astype(float)
    dt = data['dt_val']
    l = detectoutliers(data)

    #处理异常数据l
    # 1.找出0位置
    data_zero=data[(data.dt_val == 0)]
    # 2.找出负值位置
    data_minus=data[(data.dt_val < 0)]


    #补充0后的数据 对比数据
    errors_result=get_errors(data)
    df_period = errors_result[0]
    #3.找出data缺失值
    data_miss=errors_result[1]
    # data_miss=miss_result.dt_time.tolist()

    # 4.极值
    data_max=l
    if not l.empty:
        a = l.copy()
        b=pd.concat([data_zero.copy(), data_minus.copy()], axis=0,
                            ignore_index=True)
        data_max=a.append(b).drop_duplicates(subset=['dt_time'], keep=False)
    # 5.无异常数列

    # 所有异常数据
    error_all=pd.concat([data_zero, data_miss, data_minus, data_max], axis=0,
                       ignore_index=True)

    data_normal = df_period.copy().append(error_all).drop_duplicates(subset=['dt_time'], keep=False)
    # 处理结果封装成标准字段
    datas_normal=result(data_normal.dt_time.tolist(), data, df_period, 0)
    datas_zero=result(data_zero.dt_time.tolist(), data, df_period, 2)
    datas_miss=result(data_miss.dt_time.tolist(), data, df_period, 1)  # 缺失值
    datas_minus=result(data_minus.dt_time.tolist(), data, df_period, 3)
    # 极值处理
    datas_max=result(data_max.dt_time.tolist(), data, df_period, 4)

    # 汇总所有数据
    list_all=pd.concat([datas_normal, datas_zero, datas_miss, datas_minus, datas_max], axis=0,
                       ignore_index=True)
    list_all['id']=field  # 添加id列
    list_all=list_all.sort_values(by='dt_time', axis=0, ascending=True)  # 按时间排序
    list_all.reset_index()
    list_all['dt_time']=list_all['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))

    list_all['dt_type']='median'
    # 统一修正错误值
    list_all=replace_data_lg(list_all.copy())

    # 类型转换
    list_all=list_all.astype('str')
    data=data.astype('object')
    db=getConfig()
    oracleUtil(db['username'] + ':' + db['password'] + '@' + db['url'] + '/' + db['sid'], list_all, 'error_out')
    print(datetime.datetime.now())



'''差分法 测试'''

def create_arima(field,data):
    print(datetime.datetime.now())
    #原数据预处理
    data['dt_time']=data['dt_time'].astype('datetime64')
    data[['dt_val']]=data[['dt_val']].astype(float)
    # data=data.sort_values(by='dt_time', axis=0, ascending=True)  # 按时间排序
    data['dt_time']=data['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))
    # # 显示所有列
    # pd.set_option('display.max_columns', None)
    # # 显示所有行
    # pd.set_option('display.max_rows', None)

    model=None
    # 找到拐点,定义model
    inflexion=0;
    if not data[(data.dt_val != 0)].dt_val.empty:
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
        model=get_mode(obj)

    it=pd.DataFrame({'key': obj })
    it_copy=it.copy()

    # 删除/选取某列含有特定数值的行
    # 通过~取反，选取不包含数字model的行
    if (inflexion != 0):#多model不予剔除model
        it=it[~it['key'].isin(model)]

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
        end = it.tail(1)["key"].index.values[0]#最后一个不做处理
        it1=it_copy[it_copy['key'].isin(model)]

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
                            # print("前一点是异常点(双负):", row[0] - 1, it.loc[[row[0] - 1]]["key"])
                            data_index.append(row[0] - 1)
                            continue
                        #前一个点与model的差值
                        last=dist(it_copy.loc[[row[0] - 1]].values,module);

                        if (round(it_copy.loc[row[0] - 1]["key"], 3)!= module and last>0 ):
                            # print("前一个点信息:",row[0] - 1, it_copy.loc[[row[0] - 1]].key, "是异常点")
                            data_index.append(row[0] - 1)
                            continue

                        # 后一个点与model的差值
                        next=dist(it_copy.loc[[row[0] + 1]].values,module);
                        # print(it_copy.loc[row[0] + 1]["key"])
                        if (round(it_copy.loc[row[0] + 1]["key"], 3)!= module and next > 0):
                            # print("当前点信息:", row[0] , it.loc[[row[0]]]["key"], "是异常点")
                            data_index.append(row[0] )
                            continue

    if len(data_index)==1:
        data_index.clear()
    data_index=[i + 1 for i in data_index]#修正为元数据中的位置
    data_error=[]#异常点集合

    data_allindex = data.index.tolist()
    for i, val in enumerate(data_index):
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

    #处理结果封装成标准字段
    datas_normal=result(data_normal.dt_time.tolist(), data, df_period, 0)
    datas_zero=result(data_zero, data, df_period, 2)
    datas_miss = result(data_miss, data, df_period, 1)#缺失值
    datas_minus=result(data_minus, data, df_period, 3)
    #极值处理
    datas_max=result(data_max, data, df_period, 4)

    bj=None
    if not datas_max.empty:
        # 异常二次寻找
        bj=error_filter(datas_normal, datas_max)
        if not bj.empty:
            # 去掉异常数据
            datas_normal=datas_normal.append(bj).drop_duplicates(subset=['dt_time'], keep=False)


   #汇总所有数据
    list_all=pd.concat([datas_normal, datas_zero, datas_miss, datas_minus, datas_max,bj], axis=0,
                               ignore_index=True)
    list_all['id']= field # 添加id列
    list_all = list_all.sort_values(by = 'dt_time',axis = 0,ascending = True)#按时间排序
    list_all.reset_index()
    list_all['dt_time']=list_all['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))

    list_all['dt_type'] ='diff'
    #统一修正错误值
    if not data[(data.dt_val != 0)].dt_val.empty:
        list_all=replace_data_lg(list_all.copy())

    #类型转换
    list_all=list_all.astype('str')
    data=data.astype('object')
    db=getConfig()
    oracleUtil(db['username'] + ':' + db['password'] + '@' + db['url'] + '/' + db['sid'], list_all, 'error_out')
    print(datetime.datetime.now())

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
data:处理的数据
list:处理后的结果 0位置为补充时间后的数据,1位置为缺失数据
 '''
def get_errors(data):
    list=[]
    # 3.缺失数据
    # 补充了数据 ,这时补充的数据都是 0
    data2=data.copy()
    data2['dt_time']=data2['dt_time'].astype('datetime64')
    df_period=data2.resample('D', on='dt_time').sum()
    df_period=df_period.reset_index()
    list.append(df_period)
    # 求差集
    a=df_period.copy();
    b=data.copy()
    b['dt_time']=b['dt_time'].astype('datetime64')
    miss_result=a.append(b).drop_duplicates(subset=['dt_time'], keep=False)
    data_miss=miss_result
    list.append(data_miss)
    return  list

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
        if type == 0:
            dt_item["dt_eidt"]=dt_item["dt_val"].values  # 修正值
        else:
            dt_item["dt_eidt"] = 0

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



#方法选择
def fun_choice(func,val, data):
    parse_func = {
        "create_arima": create_arima,
        "create_median": create_median,
        "create_pyculiarity":create_pyculiarity
    }
    parse_func[func](val, data)  # 执行相应方法


def data_list(func,starttime,endtime):
    # 数据sql
    sql="select id,dt_time,dt_val from error_in where 1=1  and to_date(dt_time,'yyyy/mm/dd') >=  to_date('" + starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" + endtime + "','yyyy/mm/dd')" \
        "and id not in (select distinct id from error_out) order by to_date(dt_time,'yyyy/mm/dd')"
    datas=select(sql)
    # 泵站名字集合
    fields_sql="select id from error_in where 1=1 and to_date(dt_time,'yyyy/mm/dd') >=  to_date('" + starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" + endtime + "','yyyy/mm/dd') " \
         "and id not in (select distinct id from error_out) group by id"
    fields=select(fields_sql)

    # 处理成python 可识别的
    df=pd.DataFrame(list(datas), columns=['id', 'dt_time', 'dt_val'])
	threads=[];
    for i in range(len(fields)):
        val =fields[i][0]
        print(val)
        data=df[(df.id ==val)]
        data=data.drop_duplicates(subset=['dt_time'], keep='first')
        data.reset_index(drop=True, inplace=True)
         # 多线程执行方法
        t=threading.Thread(target=fun_choice, args=(func, val, data))
        # 开启线程
        t.start()
        # 添加线程到线程列表
        threads.append(t)
        if len(threads)>=10:
            statusFlag = True;
            while statusFlag:
                item_threads = []#记录已完成的线程
                for j in range(len(threads)):
                    print('线程状态',threads[j].is_alive())
                    if not threads[j].is_alive():
                        item_threads.append(threads[j])
                        statusFlag = False
                for k in range(len(item_threads)):
                    threads.remove(item_threads[k])
                    print('线程', item_threads[k], '已清理')
                if statusFlag :
                    print('休眠3秒')
                    time.sleep(3)
                else:
                     break;


#错误二次过滤
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
    endtime='2018-12-31'
    data_list("create_arima", starttime, endtime)




