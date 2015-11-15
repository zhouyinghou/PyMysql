#!/usr/bin/env python
import commands
import os
import sys

base_dir = '/export/servers/data'
app_dir = '/export/servers/app'
backup_dir = '/export/servers/data/mybackup'

def check_envir(port, version, backup_file):
    '''check whether the environment have ready'''
    _data_file='{0}/my{1}'.format(base_dir,port)
    if os.path.exists(_data_file):
        print 'The {0} instance is exists!Please change another!'.format(port)
        exit(1)

    _my_cnf='/tmp/my.cnf'
    if not os.path.exists(_my_cnf):
        print 'The my.cnf is not in /tmp,Please create it!'
        exit(1)

    #check mysql client version
    client_mysql = ('{0}/mysql-{1}').format(app_dir, version)
    print client_mysql
    if not os.path.exists(client_mysql):
        print 'The mysql client is not exists,Please install it!'
        exit(1)

    #check xtrabackup version,if version not same to the backup hosts,it wall report undo not exists and so on
    rpm_version = 'rpm -qa|grep xtrabackup'
    version = commands.getoutput(rpm_version)
    version_value = version.split('-')[2]
    if version_value != '2.2.4':
        print 'the xtarbackup version is not same to the backup host,please change it!'
        exit(0)

    #check backup file
    xtrabackup_file = ('{0}/mybackup/{1}').format(base_dir, backup_file)
    if not os.path.exists(xtrabackup_file):
        print 'Backup file not in the position,Please move it'
        exit(0)

def mkdir(port, version, backup_file):
    '''make directory'''
    check_envir(port, version, backup_file)
    _mk_backup_dir_cmd = "mkdir -p {0}/mysql{1}/".format(backup_dir, port)
    os.system(_mk_backup_dir_cmd)
    _mk_data_dir_cmd = "mkdir -p {0}/my{1}/".format(base_dir, port)
    os.system(_mk_data_dir_cmd)
    _mk_data_dir_cmd = "cd {0}/my{1} && mkdir -p binlog data ibdata iblog log run tmp".format(base_dir,port)
    print _mk_data_dir_cmd
    os.system(_mk_data_dir_cmd)

def copy_my_conf(port, version, backup_file):
    '''create my.cnf'''
    mkdir(port, version, backup_file)
    _cp_my_cnf='cp /tmp/my.cnf {0}/my{1}'.format(base_dir,port)
    os.system(_cp_my_cnf)

def restore(backup_file,port, version):
    '''restore data from full backup'''
    copy_my_conf(port, version, backup_file)
    _restore_dir = "{0}/mysql{1}".format(backup_dir,port)
    _extract_backup_cmd="tar -ixzvf {0}/{1} -C {2}".format(backup_dir, backup_file, _restore_dir)
    os.system(_extract_backup_cmd)
    _restore_cmd="innobackupex --defaults-file={0}/backup-my.cnf --apply-log --use-memory=16G {1}".format(_restore_dir,_restore_dir)
    os.system(_restore_cmd)

def recover(port):
    '''recover data from full backup'''
    _restore_dir = "{0}/mysql{1}".format(backup_dir,port)
    _recover_cmd="innobackupex --defaults-file={0}/my{1}/my.cnf --copy-back --use-memory=16G {2}".format(base_dir,port,_restore_dir)
    print _recover_cmd
    os.system(_recover_cmd)

def start_mysql(version,port):
    '''start mysql'''
    _chown_dir='chown -R mysql.myinstall {0}'.format(base_dir)
    os.system(_chown_dir)
    _start_cm='/bin/sh {0}/mysql-{1}/bin/mysqld_safe --defaults-file={2}/my{3}/my.cnf --user=mysql &'.format(app_dir,version,base_dir,port)
    os.system(_start_cm)


def incre_data(port,err_time):
    '''mysqlbinlog recover the increment dat'''
    _slave_pos_file='{0}/mysql{1}/xtrabackup_binlog_pos_innodb'.format(backup_dir,port)
    _pos_file=open("{0}","r").format(_slave_pos_file)
    _pos_file_sp=_pos_file[0].split('\t')
    _pos=_pos_file_sp[0]
    _file=int(_pos_file_sp[1])

    _mysql_bin_log='{0}/my{1}/binlog/mysql-bin*'.format(base_dir,port)
    #for _mysql_bin_file



    data_dir='/export/servers/data/my{0}/binlog'.format(port)
    #mysqlbinlog > /tmp/incre_data.sql

def main():
    if len(sys.argv) != 4:
        print '''
        Usage: python auto_recover.py backup_file port version
               eg: python auto_recover.py full_20151026060056.tar.gz 3306 5.6.23
                version: 5.5.18/5.5.36/5.6.16/5.6.23
         Note: 1. backup_file must put in /export/servers/data/mybackup
               2. my.cnf must put in /tmp
               3. Its best put my.cnf is source db,but the PORT don't forget to change
              '''
        exit(1)

    backup_file = sys.argv[1]
    port = sys.argv[2]
    version = sys.argv[3]

    restore(backup_file,port, version)
    recover(port)
    start_mysql(version,port)

if __name__ == "__main__":
    main()
