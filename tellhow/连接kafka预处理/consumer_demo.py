# -*- coding: utf-8 -*-
# @Time : 2019/11/13 15:39
# @Author : len
# @Email : ysling129@126.com
# @File : consumer_demo
# @Project : tellhow
# @description :

from kafka import KafkaConsumer

consumer = KafkaConsumer('01_BJ56000015_DZ', bootstrap_servers=[
        '10.10.10.32:21007',
        '10.10.10.33:21007',
        '10.10.10.34:21007'])
for msg in consumer:
    recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
    print (recv)