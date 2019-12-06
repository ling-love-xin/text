# -*- coding: utf-8 -*-
# @Time : 2019/11/12 16:06
# @Author : len
# @Email : ysling129@126.com
# @File : demo
# @Project : tellhow
# @description :

import json
from kafka import KafkaClient
from kafka import KeyedProducer
from kafka import KafkaProducer


producer = KafkaProducer(
    sasl_mechanism="GSSAPI",
    security_protocol='SASL_PLAINTEXT',
    # sasl_plain_username='kafkaclient',
    sasl_kerberos_service_name='kafka',
    sasl_kerberos_domain_name='hadoop.hadoop.com',
    bootstrap_servers='10.10.10.34:21007,10.10.10.33:21007,10.10.10.32:21007',
)

msg_dict = {
    "sleep_time": 10,
    "db_config": {
        "database": "bj_tz_ldjsc1.1",
        "host": "10.10.10.44",
        "user": "postgresql",
        "password": "tellhow123"
    },
    "table": "demo.emergency_event_test",
    "msg": "Hello World"
}
msg = json.dumps(msg_dict)
producer.send('test_rhj', msg, partition=0)
producer.close()
