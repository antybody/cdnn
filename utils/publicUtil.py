#-*-coding:utf-8-*-



#求众数
from sqlalchemy import NVARCHAR, Float, Integer


def get_mode(L):
    x=dict((a, L.count(a)) for a in L)
    y=[k for k, v in x.items() if max(x.values()) == v]
    return y


'''
db_url参数说明:格式为gxsy:gxsy123@120.26.116.232:1521/orcl
data：参数为dataframe类型
table:操作的数据库
'''
def oracleUtil(db_url,data,table):
        from sqlalchemy import create_engine
        from sqlalchemy.types import String
        conn_string='oracle+cx_oracle://'+db_url
        engine=create_engine(conn_string,encoding="utf-8", echo=False)
        dtypedict=mapping_df_types(data)
        data.to_sql(table, con=engine,if_exists='append',index=False,dtype=dtypedict)
        engine.execute("SELECT * FROM "+table).fetchall()
        [('test 1'), ('test 2'), ('test 3'),('test 4')]

def mapping_df_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: NVARCHAR(length=255)})
        if "float" in str(j):
            dtypedict.update({i: Float(precision=2, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: Integer()})
    return dtypedict