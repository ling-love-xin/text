# -*- coding: utf-8 -*-
# @Time : 2019/11/15 10:25
# @Author : len
# @Email : ysling129@126.com
# @File : producer_demo
# @Project : tellhow
# @description :

from pykafka import KafkaClient


host = '10.10.10.32:21007,10.10.10.33:21007,10.10.10.34:21007'
zookeeper_host = '10.10.10.34:24002,10.10.10.33:24002,10.10.10.32:24002'

client = KafkaClient(hosts=host,zookeeper_hosts=zookeeper_host)

print(client.topics)
# 生产者