import cx_Oracle
import configparser
import os
import sys

def getOrcl232Conn():
    try:
        config = configparser.ConfigParser()
        config.read(os.path.join(sys.path[0], 'dbconf.ini'))
        # config.read(sys.path[0] + '\\dbconf.ini')
        username = config.get('orcl', 'username')
        password = config.get('orcl', 'password')
        url = config.get('orcl', 'url')
        sid = config.get('orcl', 'sid')
        # conn = cx_Oracle.connect(username, password, url+'/'+sid)
        conn = cx_Oracle.connect('gxsy', 'gxsy123', '120.26.116.232:1521/orcl')
    except Exception:
        conn.close()
    else:
        return conn