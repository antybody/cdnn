import os

from cdnn.db import conns

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def db_dperate(sql,param):
    sql_l = sql.split(' ')
    func = sql_l[0]
    parse_func = {
        'insert': update(sql, param),
        'delete': update(sql, param),
        'update': update(sql, param),
        'select': select(sql, param),
    }
    res = []
    if func in parse_func:
        res = parse_func[func]  # 把切割后的 用户sql的列表 传入对应的sql命令函数里
    return res

def select(sql,param):
    conn = conns.getOrcl232Conn()
    cursor = conn.cursor()
    # sql = 'select TIMESTAMP,FP_TOTALENG FROM GXSY."cxcs"'
    try:
        cursor.execute(sql, param)
        result = cursor.fetchall()
        cursor.close()  # 关闭游标
    except Exception as err:
        raise err
    finally:
        conn.close()  # 关闭连接
    return result

def update(sql, param):
    conn = conns.getOrcl232Conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, param)
        cursor.close()  # 关闭游标
        conn.commit()  # 提交操作
        result = ['操作成功']
    except Exception as err:
        raise err
    finally:
        conn.close()  # 关闭连接
    return result

if __name__ == '__main__':
    sql = 'select TIMESTAMP,FP_TOTALENG FROM GXSY."cxcs" ' # where TIMESTAMP = :1
    param = []
    result = db_dperate(sql, param)
    for each_list in result:
        print(each_list)