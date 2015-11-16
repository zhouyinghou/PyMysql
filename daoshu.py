#! /usr/bin/env python
# -*- coding: utf8

from make_excel import write_excel
from mysql.connector import connect
from getpass import getpass
import sys

def daoshu(passwd,filename,host,db,port):
    ''''''
    sql = ''
    with open(filename) as f:
        lines = f.readlines()
        for line in lines:
            sql = sql + line

    config = {
                "host"      : host,
                "user"      : "zhouhuan",
                "passwd"    : passwd,
                "port"      : port,
                "database"  : db,
                "charset"   : "utf8"
            }
    conn = connect(**config)
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = cursor.column_names
    conn.close()
    write_excel(columns, rows, "{0}.xlsx".format(filename), sheet_name = 'Sheet1')

if __name__ == "__main__":
    if len(sys.argv)!=4:
        print "Usage: python daoshu.py filename host db_name port"
    else:
        passwd = getpass("please input password for zhouhuan:")
        filename = sys.argv[1]
        host = sys.argv[2]
        db = sys.argv[3]
        port = sys.argv[4]
        daoshu(passwd,filename,host,db,port)
