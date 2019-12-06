# -*- coding: utf-8 -*-
# @Time : 2019/11/8 15:19
# @Author : len
# @Email : ysling129@126.com
# @File : __init__.py
# @Project : tellhow
# @description :

from psycopg2 import extras

import psycopg2 as pg


pg_con = pg.connect(
    database='tellhowDB',
    user='postgres',
    password='postgresql',
    host='127.0.0.1',
    port=5432)
pg_cor = pg_con.cursor(cursor_factory=extras.RealDictCursor)
# print( '连接成功')
# pg_con.close()