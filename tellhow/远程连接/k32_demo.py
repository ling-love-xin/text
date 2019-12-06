# -*- coding: utf-8 -*-
# @Time : 2019/11/18 10:30
# @Author : len
# @Email : ysling129@126.com
# @File : k32_demo
# @Project : tellhow
# @description :
import paramiko


if __name__ == '__main__':
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname='10.10.10.32',
        port=22,
        username='root',
        password='sjkj123..')
    print("连接成功！")
    stdin, stdout, stderr = ssh.exec_command(
        'pwd;source /opt/hadoopclient/bigdata_env;echo $?;kinit -kt /root/app/kafka-importer/conf/user.keytab tellhow@HADOOP.COM;echo $?;'
    'kafka-topics.sh --list --zookeeper 10.10.10.32:24002,10.10.10.33:24002,10.10.10.34:24002/kafka;'
        )
    res, err = stdout.read(), stderr.read()
    result = res if res else err
    print(result.decode())

    print('***' * 30)

    stdin, stdout, stderr = ssh.exec_command(
        'which kinit')
    res, err = stdout.read(), stderr.read()
    result = res if res else err
    print(result.decode())
    print('***' * 30)

    # stdin, stdout, stderr = ssh.exec_command(
    #     'kafka-console-consumer.sh --topic 01_BJ56000015_DZ --bootstrap-server  10.10.10.32:21007,10.10.10.33:21007,10.10.10.34:21007 --consumer.config /home/dp/lgz/consumer.properties --from-beginning')
    # # res, err = stdout.read(), stderr.read()
    # # result = res if res else err
    # # print(result.decode())
    # # print('***' * 30)
    # #
    # #
    # # client = ssh.invoke_shell()
    # # stdin, stdout, stderr = client.send('pwd'+'\n')
    # # res, err = stdout.read(), stderr.read()
    # # result = res if res else err
    # # print(result.decode())
    #
    # # stdin, stdout, stderr = ssh.exec_command('whoami')
    # # stdin, stdout, stderr = ssh.exec_command('/opt/hadoopclient/Kafka/kafka/bin/kafka-topics.sh --list')
    # # res, err = stdout.read(), stderr.read()
    # # result = res if res else err
    # # print(result.decode())

    ssh.close()
