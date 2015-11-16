#! /usr/bin/env python
#-*- coding: utf8

import mysql.connector
import commands
import sys
import getpass


def get_mysql_conn(host, user, passwd, database, port, charset="utf8"):
    ''''''
    conn = mysql.connector.connect(host=host, user=user, passwd=passwd, \
           database=database, port=port, charset=charset)
    return conn

def install_mkpasswd():
    cmd = 'rpm -qa|grep expect'
    install = 'yum install expect'
    pack = commands.getoutput(cmd)
    if len(pack) != 0:
        commands.getoutput(install)

def make_password():
    passwd = commands.getoutput('mkpasswd -l 12 -d 4 -s 0')
    return passwd

def get_append_user(host, pwd, database, port):
    conn = get_mysql_conn(host, 'zhouhuan', pwd, database, port)
    cursor = conn.cursor()
    user_sql = "SELECT user from mysql.db where db='%s' and user not in('mydb_query','dbquery');" % database
    cursor.execute(user_sql)
    user_value = cursor.fetchone()[0]
    conn.close()
    return user_value

def get_secret(secret, pwd):
    conn = get_mysql_conn('192.168.100.7', 'zhouhuan', pwd, 'report', 3308)
    cursor = conn.cursor()
    secret_sql = "SELECT local_passwd from passwd where secret like '%"+secret+"%';"
    try:
        cursor.execute(secret_sql)
        secret_value = cursor.fetchone()[0]
        print secret_value
        return secret_value
    except Exception,e:
        print "No Record!"
    conn.close()

def append_grants(host, pwd, database, port):
    conn = get_mysql_conn(host, 'zhouhuan', pwd, database, port)
    cursor = conn.cursor()
    priv_sql = "SELECT password from mysql.user u,mysql.db db where u.user=db.user and db.db='%s';" % database
    cursor.execute(priv_sql)
    priv_value = str(cursor.fetchone()[0])
    conn.close()
    return priv_value

def grant_priv(user_value,database, iden, grant_passwd):
    ''''''
    install_mkpasswd()
    priv="GRANT SELECT, INSERT, UPDATE, DELETE ON "
    str1 = ".* TO '"
    str2 = "'@'"
    str4 = "';"
    file = open('/home/zhouhuan/script/work/ip.list')
    ips = file.readlines()
    file.close()
    for ip in ips:
        iplist = ip.strip('\n')
        seq = (priv, database, str1, user_value, str2, iplist, iden, grant_passwd, str4)
        grant_sql=''.join(seq)
        print grant_sql

def put_pass(org):
    '''将密码insert到report数据库passwd表'''
    secret = get_secret_pass(org)
    conn_mysql = get_mysql_conn('192.168.100.6', 'dbchecksum', '', 'report', 3308)
    cursor = conn_mysql.cursor()
    put_sql = '''
        insert into passwd
        (local_passwd, secret, create_time)
        values ('%s', '%s', sysdate())''' % (org, secret)
    cursor.execute(put_sql)
    conn_mysql.commit()
    cursor.close()
    conn_mysql.close()

def get_secret_pass(org):
    '''将密码insert到report数据库passwd表'''
    conn_mysql = get_mysql_conn('192.168.100.6', 'dbchecksum', '', 'report', 3308)
    cursor = conn_mysql.cursor()
    get_sql = '''select password('%s')''' % (org)
    cursor.execute(get_sql)
    row=cursor.fetchone()[0]
    return row
    cursor.close()
    conn_mysql.close()

def get_mydb_priv(database):
    '''query machine'''
    priv="GRANT SELECT ON "
    str1 = ".* TO 'mydb_query'@'"
    query_host1='192.168.100.4'
    query_host2='192.168.100.5'
    grant_passwd = "' IDENTIFIED BY PASSWORD  '*172683C3E0FE4E1392B4651ACD1183046DE1435D';"
    seq1 = (priv, database, str1, query_host1, grant_passwd)
    seq2 = (priv, database, str1, query_host2, grant_passwd)
    grant_sql=''.join(seq1)
    grant_sql2=''.join(seq2)
    print grant_sql,grant_sql2
    return grant_sql,grant_sql2

def get_query_priv(database, plant, query_host):
    '''query machine'''
    priv="GRANT SELECT ON "
    str1 = ".* TO 'dbquery'@'"
    grant_passwd = "' IDENTIFIED BY PASSWORD  '*3467698CAACFE9C78647476BDA81519266AC90D2';"
    seq = (priv, database, str1, query_host, grant_passwd)
    grant_sql=''.join(seq)
    return grant_sql
    print grant_sql

def grant_query_priv(host, password, port, database, plant, query_host):
    '''授予数据库查询权限'''
    conn_mysql = get_mysql_conn(host, 'zhouhuan', password, 'test', port)
    cursor = conn_mysql.cursor()
    _grant_sql = get_query_priv(database, plant, query_host)
    cursor.execute(_grant_sql)
    cursor.close()
    conn_mysql.commit()
    conn_mysql.close()

def main():
    if (len(sys.argv) == 5) and (sys.argv[1]=='append'):
        iden = "' IDENTIFIED BY PASSWORD '"
        pwd=getpass.getpass("Enter zhouhuan's passwd:")
        host = sys.argv[2]
        database = sys.argv[3]
        port = sys.argv[4]
        grant_passwd = append_grants(host, pwd, database, port)
        user_value = get_append_user(host, pwd, database, port)
        grant_priv(user_value,database, iden, grant_passwd)
    elif (len(sys.argv) == 5) and (sys.argv[1]!='append'):
        password = getpass.getpass("Enter zhouhuan's passwd:")
        host = sys.argv[1]
        database = sys.argv[2]
        port = sys.argv[3]
        plant = sys.argv[4]

        if plant == 'jr':
            query_host = 'host2'
        elif plant != 'jr':
            query_host = '192.168.100.3'
        grant_query_priv(host, password, port, database, plant, query_host)
    elif len(sys.argv) == 3 and sys.argv[1]!='secret' and sys.argv[1]!='mydb':
        db=sys.argv[1]
        user_value=sys.argv[2]
        iden = "' IDENTIFIED BY '"
        grant_passwd = make_password()
        grant_priv(user_value, db, iden, grant_passwd)
        put_pass(grant_passwd)   
    elif (len(sys.argv) == 3 and sys.argv[1]=='mydb'):
        database = sys.argv[2]
        get_mydb_priv(database)
    elif len(sys.argv) == 3 and sys.argv[1]=='secret':
        secret = sys.argv[2]
        pwd=getpass.getpass("Enter zhouhuan's passwd:")
        get_secret(secret,pwd)
    else:
        print '''Usage: 
                function 1: python grants.py db user
                        make new password
                function 2: python grants.py append host database port
                        apply to the old secret, make the privileges
                function 3: python grants.py host database port jr/yz/wy
                        grant the dbquery privileges
                function 4: python grants.py mydb database 
                        grant the mydb privileges
                function 5: python grants.py secret encrypt_secret
                        apply to the encrypt_secret,get the secret
              '''

if __name__ == "__main__":
    main()
