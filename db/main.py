import os
import cx_Oracle

from cdnn.db import conns

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

'''
  数据查询
'''
def select(sql):
    orclConfig = conns.getConfig()
    conn = cx_Oracle.connect(orclConfig['username'], orclConfig['password'], orclConfig['url'] + '/' + orclConfig['sid'])
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        result = cursor.fetchall()

    except Exception as err:
        raise err
    finally:
        cursor.close()  # 关闭游标
        conn.close()  # 关闭连接
    return result



# def update(sql, param):
#     conn = conns.getOrcl232Conn()
#     cursor = conn.cursor()
#     try:
#         cursor.execute(sql, param)
#         cursor.close()  # 关闭游标
#         conn.commit()  # 提交操作
#         result = ['操作成功']
#     except Exception as err:
#         raise err
#     finally:
#         conn.close()  # 关闭连接
#     return result

if __name__ == '__main__':
    starttime = '2017-01-01'
    endtime = '2017-12-30'

    sql = "select * from error_in where 1=1 and to_date(dt_time,'yyyy/mm/dd') >=  to_date('"  +starttime + "','yyyy/mm/dd') and  to_date(dt_time,'yyyy/mm/dd') <= to_date('" +endtime+"','yyyy/mm/dd')"
    param = []
    result = select(sql)
    print(result)
    # for each_list in result:
    #     print(each_list)