# -*- coding: utf-8 -*-
# @Time : 2019/11/8 16:03
# @Author : len
# @Email : ysling129@126.com
# @File : demo
# @Project : PycharmProjects
# @description :


import psycopg2 as pg
import pandas as pd

pg_con = pg.connect(
    database='bj_tz_ldjsc1.1',
    user='uetl',
    # password='tellhow123',
    host='10.10.10.44',
    port=5432)
print('连接成功')

data = pd.read_sql('SELECT * FROM biz_origin.o_zwfwj_pal_cst_rank_num  ;', con=pg_con)
print(data)
pg_con.close()
print('断开连接')
