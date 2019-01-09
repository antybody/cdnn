
import configparser
import os
import sys

dbConfig = {}

def getConfig():
    try:
        config = configparser.ConfigParser()
        curpath = os.path.dirname(os.path.realpath(__file__))

        config.read(os.path.join(curpath, 'dbconf.ini'))
        # config.read(sys.path[0] + '\\dbconf.ini')
        dbConfig = {
          'username' :config.get('orcl', 'username'),
          'password': config.get('orcl', 'password'),
          'url': config.get('orcl', 'url'),
          'sid': config.get('orcl', 'sid')
        }
        # conn = cx_Oracle.connect(username, password, url+'/'+sid)
        # conn = cx_Oracle.connect('gxsy', 'gxsy123', '120.26.116.232:1521/orcl')
        return dbConfig
    except Exception:
        return {}