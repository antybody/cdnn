import datetime
from cdnn.db.conns import getConfig
from  cdnn.models.median import detectoutliers
import pandas as pd

# 数据库相关
from cdnn.db.main import select

from cdnn.utils.publicUtil import  oracleUtil

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
    list_all.reset_index(drop=True, inplace=True)
    list_all['dt_time']=list_all['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))

    list_all['dt_type']='median'
    # 统一修正错误值??????
    dt_error=pd.concat([datas_miss, datas_max], axis=0,
                       ignore_index=True)
    list_all = amendment(dt_error,list_all)#修正错误
    # 类型转换
    list_all=list_all.astype('str')
    data=data.astype('object')
    db=getConfig()
    oracleUtil(db['username'] + ':' + db['password'] + '@' + db['url'] + '/' + db['sid'], list_all, 'error_out8')
    print(datetime.datetime.now())

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

        if type == 0 or type ==2 or type == 3:
            dt_item["dt_reason"]=0
            dt_item["dt_eidt"]=dt_item["dt_val"].values  # 修正值
        else:
            dt_item["dt_reason"]=type  # 添加异常类型
            dt_item["dt_eidt"] = 0

        if type != 1:  # 如果不是缺失数据就填充其原始数据
            dt_item_old=old_data[(old_data.dt_time == str(values))].copy()  # 获取一行数据传入原数据
            dt_item["dt_val"]=dt_item_old["dt_val"].values  # 老数据中的val
        else:
            dt_item["dt_val"]=''  # 老数据中的val
        if i == 0:
            result=dt_item.copy()
            continue
        result=result.append(dt_item, ignore_index=True)
    return result

'''
 #修正错核心方法
 dt_error:要修正的数列集合
 list_all:总数据
 '''
def amendment(dt_error,list_all):
    if not dt_error.empty:
        dt_error['dt_time']=dt_error['dt_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y/%m/%d'))
        # 循环找 异常数据 (缺失数据 和 极值数据 ) 并替换为修正值
        for i in range(0, len(dt_error)):
            dt_item=list_all[(list_all.dt_time == str(dt_error.iloc[i]['dt_time']))].copy();
            # 获取当前行的index
            list_index=dt_item.index.values[0]

            # 0位置不做处理
            if (list_index == 0):
                continue;
            if (list_index <= 2):
                item_val=0.0;
                for j in range(0, list_index):
                    item_val=item_val + \
                             list_all[(list_all.dt_time == getTime(str(dt_error.iloc[i]['dt_time']), -(j + 1)))][
                                 "dt_eidt"].values[0]
                dt_item["dt_eidt"]=item_val / list_index
            else:
                #取当前数据的前3位平均值 作为修正值
                item_val=0.0;
                for j in range(1, 4):
                    item_val=item_val + list_all[(list_all.dt_time == getTime(str(dt_error.iloc[i]['dt_time']), -j))][
                        "dt_eidt"].values[0]
                dt_item["dt_eidt"]=item_val / 3
            dt_item["dt_eidt"]=round(dt_item["dt_eidt"],2)
            list_all[(list_all.dt_time == str(dt_error.iloc[i]['dt_time']))]=dt_item
    return list_all

'''
 #时间加减的操作
 t_str:要处理的时间
 day:要加减的天数
 '''
def getTime(t_str,day):
    time=datetime.datetime.strptime(t_str, '%Y/%m/%d')
    delta=datetime.timedelta(days=day)
    n_days=time + delta
    return n_days.strftime('%Y/%m/%d')


#方法选择
def fun_choice(func,val, data):
    parse_func = {
        "create_median": create_median
    }
    parse_func[func](val, data)  # 执行相应方法


def data_list(func,starttime,endtime):
    # 数据sql
    sql="select id,dt_time,dt_val from error_in where 1=1  and to_date(dt_time,'yyyy/mm/dd') >=  to_date('" + starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" + endtime + "','yyyy/mm/dd') order by to_date(dt_time,'yyyy/mm/dd')"
    datas=select(sql)
    # 泵站名字集合
    fields_sql="select id from error_in where 1=1 and to_date(dt_time,'yyyy/mm/dd') >=  to_date('" + starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" + endtime + "','yyyy/mm/dd') group by id"
    fields=select(fields_sql)

    # 处理成python 可识别的
    df=pd.DataFrame(list(datas), columns=['id', 'dt_time', 'dt_val'])
    for i in range(len(fields)):
        val =fields[i][0]
        data=df[(df.id ==val)]
        data.index=[i for i in range(len(data))]
        fun_choice(func, val, data)

if __name__ ==  '__main__':
    starttime='2017-01-01'
    endtime='2017-12-30'
    data_list("create_median",starttime,endtime)






