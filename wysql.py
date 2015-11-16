#! /usr/bin/env python
# -*- coding: utf8

from cmd import Cmd
import sys
import os
import getpass
import cluster_loader
import conn
from Color import Color
import os


class Client(Cmd):
    ''' '''
    prompt = "no cluster selected> "
    ruler = "-"

    def __init__(self, clusters, user, pwd):
        Cmd.__init__(self)
        self.clusters = clusters
        self.user = user
        self.pwd = pwd
        self.cureent_instance = None
        self.current_connection = None
        self.cursor = None

   
    def do_exit(self, args):
        ''' '''
        print args
        sys.exit()

   
    def do_EOF(self, args):
        ''' '''
        return True


    def emptyline(self):
        ''' '''
        pass


    def run_on_instance(self, instance, sql, autocommit=False):
        ''' '''
        cnx = conn.get_conn(instance, self.user, self.pwd)
        cursor = cnx.cursor()
        if autocommit:
            cursor.execute("set autocommit=1")

        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = cursor.column_names
        cnx.close()

        return rows, columns


    def run_on_cluster(self, sql, on_slave=True):
        ''' '''
        results = {}
        columns = {}
        for cluster_name in sorted(clusters.keys()):
            cluster = clusters[cluster_name]
            if on_slave:
                rows, cols= self.run_on_instance(cluster.slave, sql)
            else:
                rows, cols= self.run_on_instance(cluster.master, sql)

            results[cluster_name] = rows
            columns[cluster_name] = cols

        return results, columns


    def run_on_slaves(self, sql):
        ''' '''
        return self.run_on_cluster(sql)


    def run_on_masters(self, sql):
        ''' '''
        return self.run_on_cluster(sql, False)


    def print_cluster(self, cluster):
        ''' '''
        print cluster.name
        print "\t", cluster.master.get_summary()
        print "\t", cluster.slave.get_summary()
        print
         

    def do_list(self, args):
        '''Lists all clusters'''
        index = 0
        prefix = ""
        if args is not None:
            prefix = args.lower()

        for cluster_name in sorted(clusters.keys()):
            if cluster_name.startswith(prefix):
                cluster = clusters[cluster_name]
                self.print_cluster(cluster)
                index += 1
           

    def help_use(self):
        ''' '''
        print "Usage: use cluster_name [slave|master]"


    def complete_use(self, text, line, begidx, endidx):
        ''' '''
        if not text:
            return clusters.keys()

        return [cluster for cluster in clusters.keys() if cluster.startswith(text)]


    def do_use(self, args):
        ''''''
        if args == "":
            self.help_use()
            return

        items = args.split()

        cluster_name = items[0]
           
        if cluster_name not in clusters:
            print "Cluster name {0} is not in this cluster list, please run 'list' for details.".format(cluster_name)
            return
           
        self.current_instance = clusters[cluster_name].slave

        if len(items) == 2 and items[1].lower() == "master":
            self.current_instance = clusters[cluster_name].master
         
        if self.current_connection is not None:
            conn.close_conn(self.current_connection)
        self.prompt = "{0}> ".format(self.current_instance.get_summary())


        self.current_connection = conn.get_conn(self.current_instance, self.user, self.pwd)
        self.cursor = self.current_connection.cursor()

   
    def do_slaves(self, line):
        ''' '''
        sql = "show slave status"
        results, columns = self.run_on_slaves(sql)
        print "Cluster\tIO\tSQL\tSec_bhd"
        print self.ruler * 40
        for cluster_name in results:
            row = results[cluster_name][0]
            cols = columns[cluster_name]
            d = dict(zip(cols, row))

            print "{0}\t{1}\t{2}\t{3}".format(d["Slave_IO_Running"], d["Slave_SQL_Running"], d["Seconds_Behind_Master"], cluster_name)



    def do_test(self, args):
        ''' '''
        sql = "select 1"
        try:
            for cluster_name in sorted(clusters.keys()):
                cluster = clusters[cluster_name]
                for instance in (cluster.master, cluster.slave):
                    if self.run_on_instance(cluster.master, sql) is not None:
                        print "OK:\t{0}".format(instance.get_summary())
           
        except Exception, e:
            print instance.get_summary(), e


    def do_mysql(self, args):
        ''' '''
        ins = self.current_instance
        cmd = "mysql -h{0} -u{1} -p{2} -P{3} -D{4} -A".format(ins.host, self.user, self.pwd, ins.port, ins.database)
        os.system(cmd)


    def do_csv(self, args):
        ''' '''
        sql = args
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            for row in rows:
                for item in row:
                    print "\"{0}\",".format(item),
                print


        except Exception, e:
            print e


   
    def do_find(self, args):
        ''' '''
        for cluster in clusters.values():
            if cluster.contains(args):
                self.print_cluster(cluster)


    def do_var(self, args):
        ''' '''
        if args is None:
            return

        sql = "select @@{0}".format(args)
        m_rows, m_cols = self.run_on_masters(sql)
        s_rows, s_cols = self.run_on_slaves(sql)
       
        padding = 15
        print "{0}{1}{2}".format("cluster".ljust(padding), "master".rjust(padding), "slave".rjust(padding))
        print self.ruler * padding  * 3
        for cluster_name in clusters:
            m_var = m_rows[cluster_name][0][0]
            s_var = s_rows[cluster_name][0][0]
            info = "{0}{1}{2}".format(cluster_name.ljust(padding), str(m_var).rjust(padding), str(s_var).rjust(padding))
            if m_var != s_var:
                info = Color.warning(info)

            print info

   
    def do_master(self, args):
        ''' '''
        sql = args
        rows, cols = self.run_on_masters(sql)
        for cluster in clusters:
            print cluster, rows[cluster]


    def default(self, line):
        ''' '''
        try:
            if self.current_instance.type == "slave" and not (
                line.lower().startswith("select") or
                line.lower().startswith("show") or  
                line.lower().startswith("desc") or  
                line.lower().startswith("explain")):
                print "Error: Only select, show, desc and explain queries can be executed on slave."
                return

            self.cursor.execute(line)
            rows = self.cursor.fetchall()
            cols = self.cursor.column_names
            for col in cols:
                print col, "\t",
            print

            for row in rows:
                for item in row:
                    print item,"\t",
                print


        except Exception, e:
            print e


    def postcmd(self, stop, line):
        ''' '''
        print


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: wysql username"
        exit(1)

    user = sys.argv[1]
    pwd  = getpass.getpass("Enter {0}'s pass:".format(user))

    path = os.path.split( os.path.realpath(sys.argv[0]) )[0]
    clusters = cluster_loader.get_clusters(path + "/cluster.cfg")
    client = Client(clusters, user, pwd)
    client.cmdloop("Welcome to WySQL Console.")
        
