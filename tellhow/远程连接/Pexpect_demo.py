# -*- coding: utf-8 -*-
# @Time : 2019/11/19 15:55
# @Author : len
# @Email : ysling129@126.com
# @File : Pexpect_demo
# @Project : tellhow
# @description :

import pexpect
import optparse

PROMPT = ['#', '$', '>', '\$', '>>>']


def send_command(child, cmd):
    child.sendline(cmd)
    child.expect(PROMPT)
    print(child.before)


def createChildSession(host, username, password):
    command = 'ssh ' + username + '@' + host
    child = pexpect.spawn(command)
    ret = child.expect([pexpect.TIMEOUT, 'Are you sure you want to continue connecting', '[P|p]assword'] + PROMPT)
    if ret == 0:
        print('[-] Error Connecting')
        return
    if ret == 1:
        child.sendline('yes')
        ret = child.expect([pexpect.TIMEOUT, '[p|P]assword'])
        if ret == 0:
            print('[-] Error Connecting')
            return
        if ret == 1:
            send_command(password)
            return
    if ret == 2:
        send_command(password)
        return
    return child


def main():
    parse = optparse.OptionParser('Usage %prog -H <host> -u <username> -p <password> -c <command>')
    parse.add_option('-H', dest='host', type='string', help='specify the host')
    parse.add_option('-u', dest='username', type='string', help='specify the username')
    parse.add_option('-p', dest='password', type='string', help='specify the password')
    parse.add_option('-c', dest='command', type='string', help='specify the command')

    (options, args) = parse.parse_args()
    host = options.host
    username = options.username
    password = options.password
    command = options.command

    session = createChildSession(host, username, password)
    send_command(session, command)


if __name__ == '__main__':
    main()