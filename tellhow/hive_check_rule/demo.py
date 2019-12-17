# -*- coding: utf-8 -*-
# @Time : 2019/12/9 11:06
# @Author : len
# @Email : ysling129@126.com
# @File : demo
# @Project : tellhow
# @description : ip

from impala.dbapi import connect
from impala.util import as_pandas
conn=connect(host='10.10.10.32', port=24002,user='bjdsj',password='Bjdsj@123', auth_mechanism="LDAP", kerberos_service_name="hive")
cursor = conn.cursor()
cursor.execute('show databases')
print(as_pandas(cursor))

from pyhive.hive import connect
# from pyhive.hive import connect
# con = connect(host='10.10.10.32',port=24002,auth='KERBEROS',kerberos_service_name="hive")
# cursor = con.cursor()
# cursor.execute('show databases')
# datas = cursor.fetchall()
# print(datas)
# cursor.close()
# con.close()

