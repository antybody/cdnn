#-*-coding:utf-8-*-
from cdnn.db.conns import getConfig
from  cdnn.models.median import detectoutliers
import pandas as pd

# 数据库相关
from cdnn.db.main import select

from cdnn.utils.publicUtil import  oracleUtil

import matplotlib.pyplot as plt

# 显示所有行
pd.set_option('display.max_rows', None)

sql = "select id,dt_time,dt_val,dt_eidt,dt_reason from error_out8 where 1=1   order by to_date(dt_time,'yyyy/mm/dd')"
datas = select(sql)

# 处理成python 可识别的
df = pd.DataFrame(list(datas), columns=['id', 'dt_time', 'dt_val','dt_eidt','dt_reason'])

d_99 = df[df['id'] =='652']
print(d_99)
# 生成测试图表
plt.figure()
plt.plot(d_99['dt_val'].astype(float), color='g')
# plt.plot(d_99['dt_eidt'].astype(float), color='r')
plt.show()