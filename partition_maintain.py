#! /usr/bin/env python
#-*- coding: utf8
#maintain the parititon table,including add three days from now partition,
#and drop the four days' ago partition

import datetime
import mysql.connector

def get_mysql_conn(host, user, passwd, database, port, charset="utf8"):
    ''''''
    conn = mysql.connector.connect(host=host, user=user, passwd=passwd, \
           database=database, port=port, charset=charset)
    return conn

def drop_partition(table_name, drop_part_time):
    ''''''
    sql = "ALTER TABLE {0} DROP PARTITION {1};".format(table_name, drop_part_time)
    print sql
    return sql

def part_value(PART_TABLE):
    ''''''
    conn = get_mysql_conn('localhost', 'user', 'password', 'information_schema', 3306)
    cursor = conn.cursor()

    for PAR in PART_TABLE:
        PART_SQL = "select PARTITION_NAME from information_schema.PARTITIONS where TABLE_NAME='{0}';".format(PAR)
        cursor.execute(PART_SQL)
        PART_VALUE = cursor.fetchall()
    conn.close()

    return PART_VALUE

def maintain_partition(TOMORROW, AFTERNOON, FIVE_DAYS_AGO, PART_TABLE):
    ''''''
    conn = get_mysql_conn('localhost', 'user', 'password', 'db', 3306)
    cursor = conn.cursor()

    drop_time = FIVE_DAYS_AGO
    for PAR in PART_TABLE:
        try:
            for _part_value in part_value(PART_TABLE):
                _part = _part_value[0]
                drop_part = 'p_' + drop_time
                if (_part < drop_part and _part is not None):
                    cursor.execute(drop_partition(PAR, _part))
                    #drop_partition(PAR, _part)
        except Exception, e:
            print drop_partition(PAR, FIVE_DAYS_AGO)
    conn.close()

if __name__ == "__main__":
    ''''''
    TODAY = datetime.date.today()
    TOMORROW = (TODAY + datetime.timedelta(days=3)).strftime('%Y%m%d')
    AFTERNOON = (TODAY + datetime.timedelta(days=4)).strftime('%Y%m%d')
    FIVE_DAYS_AGO = (TODAY - datetime.timedelta(days=4)).strftime('%Y%m%d')
    PART_TABLE = ['a_jdjr_jrappbigdata_usrcp_a_d']

    maintain_partition(TOMORROW, AFTERNOON, FIVE_DAYS_AGO, PART_TABLE)
