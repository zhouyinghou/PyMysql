#! /usr/bin/env python
#-*- coding: utf8

#monitor mysql slave status

import mysql.connector
import commands
import sys
import getpass


def get_mysql_conn(host, user, passwd, database, port, charset="utf8"):
    ''''''
    conn = mysql.connector.connect(host=host, user=user, passwd=passwd, \
           database=database, port=port, charset=charset)
    return conn

def get_port():
    port=[]
    return port

def monitor():
    '''monitor mysql slave status'''
    try:
        _port=get_port()
        for p in _port:
            conn = get_mysql_conn('127.0.0.1', 'monitor', 'monitor', 'test', p)
            cursor = conn.cursor()
            slave_sql = "show slave status"
            cursor.execute(slave_sql)
            slave_value = cursor.fetchall()
            Slave_IO = slave_value[0][10]
            Slave_SQL = slave_value[0][11]
            SecondsBehindMaster = slave_value[0][32]

            if (Slave_IO != "Yes"):
                print "1 Slave_IO_Running Slave_IO_Running=%s;Yes;No;\
                    Slave_IO_Running process is not Running!" % (Slave_IO)
            elif (Slave_SQL != "Yes"):
                print "4 Slave_SQL_Running Slave_SQL_Running=%s;Yes;No;\
                    Slave_SQL_Running process is not Running!" % (Slave_SQL)
            elif (SecondsBehindMaster > 1800):
                print "2 Mysqld_slave_lag Mysqld_slave_lag=%s;0;1800;\
                    Mysqld_slave_lag is more than 1800s !" % (SecondsBehindMaster)
            elif (SecondsBehindMaster == "NULL"):
                print "3 Mysqld_slave_lag Mysqld_slave_lag=%s;0;1800;\
                    Mysqld_slave is stop !" % (SecondsBehindMaster)
            else:
                print "0 Mysqld_slave_lag Mysqld_slave_lag=%s;0;1800;\
                    Mysqld_slave_lag is OK!" % (SecondsBehindMaster)
    except Exception, e:
        pass

def main():
    monitor()

if __name__ == "__main__":
    main()
