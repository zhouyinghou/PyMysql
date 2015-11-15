#!/usr/bin/env python
# -*- coding:utf-8 -*-
'''
    Created on 2015-02-26
    Modified on 2015-03-9
    Usage:load database data
'''

import mysql.connector
import commands
import datetime
import time
import os
from mysql.connector.constants import ClientFlag

def get_mysql_conn(host, user, passwd, database, port, charset="utf8"):
    ''''''
    conn = mysql.connector.connect(host=host, user=user, passwd=passwd, \
           database=database, port=port, charset=charset, client_flags=[ClientFlag.LOCAL_FILES])
    return conn

def load_data(date_dir,table_name):
    '''the tables' rows isn't change, load data use "replace" mode'''
    _conn = get_mysql_conn('192.168.100.1', 'houzi', 'password', 'db', 3306)
    _cursor = _conn.cursor()
    #difine the load script
    _sql = '''
        LOAD DATA LOCAL INFILE '/{0}/{1}.txt'
                REPLACE INTO TABLE {1} fields terminated by '^@#$';
        '''.format(date_dir,table_name, table_name)
    print _sql
    _cursor.execute(_sql)
    _conn.commit()
    _cursor.close()
    _conn.close()

def fetchone(host,user,passwd,database,port,sql):
    '''fetch one value, eg:the status of the load data'''
    conn = get_mysql_conn(host,user,passwd,database,port)
    cursor = conn.cursor()
    cursor.execute(sql)
    #if the value is null, then return ''
    try:
        value = cursor.fetchone()[0]
        return value
    except Exception,e:
        value = ''
        return value
    cursor.close()
    conn.close()

def load_status(date_dir,table_name):
    '''get the status(Succeed|Failed) from the statistic table,'''
    #define get the load status's script
    status_sql = '''select distinct status from load_report \
                where load_date_dir='{0}' and \
                table_name='{1}' and status = 'Succeed';
                '''.format(date_dir,table_name)
    _status = fetchone('192.168.100.2', 'dbchecksum', '', 'report', 3308, status_sql)
    return _status

def max_succeed_load_dir(table_name):
    '''get the max Succeed load directory from the statistic table,'''
    #define get the load status's script
    dir_sql = '''select max(load_date_dir) from load_report
                where table_name='{0}' and status = 'Succeed';
                '''.format(table_name)
    _max_succ_dir = fetchone('192.168.100.2', 'dbchecksum', '', 'report', 3308, dir_sql)
    try:
        #_load_dir = int(_max_succ_dir)+1
	result=datetime.datetime(*time.strptime(_max_succ_dir,"%Y%m%d")[:4])+datetime.timedelta(days=1)
        _load_dir = result.strftime("%Y%m%d")
    except Exception, e:
	_load_dir = ''
    return _load_dir

def report_sql(sql):
    '''get the information of load data'''
    conn = get_mysql_conn('192.168.100.2', 'dbchecksum', '', 'report', 3308)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def load_time():
    '''record the load's begin time,end time,consume time'''
    time_sql = 'select now()'
    time = fetchone('192.168.100.2', 'dbchecksum', '', 'report', 3308, time_sql)
    return time

def have_load():
    '''if have load process now,then do nothing'''
    try:
        id_sql = "select id from information_schema.processlist \
                where upper(Info) like 'LOAD DATA LOCAL INFILE%' and Command = 'Query';"
        id = fetchone('192.168.100.1', 'houzi', 'password', 'houzi', 3306, id_sql)
        return id
    except Exception,ee:
        id = ''
        return id

def file_status(date,table_name):
    '''whether the txt and md5 file is exists'''
    dir = '/upload'
    txt_file = dir+'/'+date+'/'+table_name+'.txt'
    md5_file = dir+'/'+date+'/'+table_name+'.md5'
    for file in txt_file,md5_file:
        if os.path.exists(file):
            status = 'YES'
        else:
            status = 'NO'
    return status

def report_succeed(date_dir, table, rows):
    '''report the load result is Succeed'''
    begin_time = load_time()
    load_data(date_dir,table)
    end_time = load_time()
    total_time = end_time - begin_time
    RESULT_S = '''insert into load_report(report_type_id, time, load_date_dir,
        table_name, row, begin_time, end_time, total, status) values(
        8, curdate(), '{0}', '{1}', {2}, '{3}', '{4}', '{5}', '{6}');
        '''.format(date_dir,table,rows,begin_time,end_time,total_time,'Succeed')
    report_sql(RESULT_S)

def report_failed(date_dir, table):
    '''report the load result is failed'''
    begin_time = load_time()
    RESULT_F = '''insert into load_report(report_type_id, time, load_date_dir,
            table_name, row, begin_time, end_time, total, status) values(
            8, curdate(), '{0}','{1}', 0, '{2}', '', '', 'Failed!File not exists!Waiting send to sftp server...');
            '''.format(date_dir,table,begin_time)
    report_sql(RESULT_F)

#def load_date():
#    '''scan the directory,then whether each dir is load Succeed,
#       if the dir isn't load,then return it'''
#    dir = '/export/sftp/wyrisk-ftp/upload'
#    TABLE_NAME = ['foton_jrb_bus_new', 'foton_qpay_immune_rule_stat_new', \
#               'foton_sku_payus_user_stat_new', 'foton_trustip_deviceid_new', \
#               'wallet_jrb_account_info_new', 'foton_order_kpi_stat_new', ]
#    try:
#        for table in TABLE_NAME:
#            #Because the directory is number, so sort the date direcotry,
#            # then from small to big load the data
#            files = os.listdir(dir)
#            files.sort(key=lambda x:int(x))
#            for _dir in files:
#                if load_status(_dir,table) != 'Succeed':
#                    return _dir
#    except Exception,e:
#        print e

def maintain_data():
    '''whether load'''
    TABLE_NAME = ['table1', 'table2', \
               'table3', 'table4', ]
    for table in TABLE_NAME:
        try:
	    #if first load, comment the max_succeed,
	    #load_dir = '20150310'
	    load_dir = str(max_succeed_load_dir(table))
            root_dir = '/upload/'
            #compute the txt file's rows
            rows = len(open('{0}{1}/{2}.txt'.format(root_dir,load_dir,table)).readlines())
            #read the md5 file's rows
            md5_rows = commands.getoutput('cat /upload/{0}/{1}.md5'.\
                        format(load_dir,table))
            '''
            Following the judgment logic:
            1 if the txt or md5 file is not exists,then do nothing;
            2 Because generate the txt file first,then transmit the md5 file,
            so if the md5 file is null,then do nothing;
            3 if there is not load process,and the table have not loaded Succeed,
                and compare md5 file to txt file's rows is equal,
                then load the table,and record ti to database
            '''
            if file_status(load_dir,table) == 'NO':
                pass
            elif md5_rows == '':
                pass
	    elif load_status(load_dir,table) == 'Succeed':
                pass
            elif have_load() == '' and file_status(load_dir,table) == 'YES':
                report_succeed(load_dir,table,rows)
            else:
                pass
        except Exception, e:
            report_failed(load_dir,table)
            print e

def main():
    ''''''
    maintain_data()

if __name__ == "__main__":
    ''''''
    main()