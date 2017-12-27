# -*- coding=utf-8 -*-
from __init__ import APP_ENV

if APP_ENV == 'dev':
    DB_SERVER = 'MySQLdb'

    DB_CONNECT = {
        'db': 'webspider',
        'user': 'username',
        'passwd': '***',
        'host': 'localhost',
        'port': 3306,
        'charset': 'utf8',
        'use_unicode': True
    }


    DB_CONNECT_R = {
        'db': 'webspider',
        'user': 'username',
        'passwd': '***',
        'host': 'localhost',
        'port': 3306,
        'charset': 'utf8',
        'use_unicode': True
    }

elif APP_ENV == 'prod':
    DB_SERVER = 'MySQLdb'

    DB_CONNECT = {
        'db': 'webspider',
        'user': 'root',
        'passwd': '123456',
        'host': 'localhost',
        'port': 3306,
        'charset': 'utf8',
        'use_unicode': True
    }


    DB_CONNECT_R = { 
        'db': 'webspider',
        'user': 'root',
        'passwd': '123456',
        'host': 'localhost',
        'port': 3306,
        'charset': 'utf8',
        'use_unicode': True
    }

