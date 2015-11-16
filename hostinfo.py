#!/usr/bin/env python
import os
import socket
import psutil
import platform
import commands

class hostInfo():
    ''''''
    def __init__(self):
        pass

    def base_info(self):
        ''''''  
        vipCmd='''/sbin/ip addr |awk '/inet/{print $0}'|awk '/global secondary/\
        {print $0}'|awk -F' ' '{print $2}'|awk -F'/' '{print $1}\''''
        self.baseInfo = {'hostname': socket.gethostname(),
                         'localIp': socket.gethostbyname(socket.gethostname()),
                         'vip': commands.getoutput(vipCmd),
                         'os': platform.linux_distribution()[0],
                         'osVersion': platform.linux_distribution()[1],
                         'bit': platform.architecture()[0],
                         'processor': platform.uname()[-1],
                         'cpuCount': psutil.cpu_count(),
                         'platVersion': platform.release(),
                        }
        return self.baseInfo
   
    def disk_info(self):
        '''display the highest usage disk's usage'''
        par = psutil.disk_partitions()
        for disk_par in par:
            disk_list = disk_par[1]
            disk_total = psutil.disk_usage(disk_list).total
            disk_used = psutil.disk_usage(disk_list).used
            per_disk_usage = psutil.disk_usage(disk_list).percent
            diskInfo = {'disk_list': disk_list,
                        'disk_total': disk_total/1024/1024/1024,
                        'disk_used': disk_used/1024/1024/1024,
                        'per_disk_usage':per_disk_usage,
                       }
            return diskInfo
   
    def memory_info(self):
        ''''''
        mem=psutil.virtual_memory()
        swap=psutil.swap_memory()
        memoryInfo = {'totalMem': int(mem.total)/1024/1024/1024,
                      'userdMem': int(mem.used)/1024/1024/1024,
                      'freeMem': int(mem.free)/1024/1024/1024,
                      'memUsage': psutil.virtual_memory().percent,
                      'totalSwap': int(swap.total)/1024/1024/1024,
                      'userdSwap': int(swap.used)/1024/1024/1024,
                      'freeSwap': int(swap.free)/1024/1024/1024,
                     }
        return memoryInfo
   
    def network_info(self):
        ''''''
        net=psutil.net_io_counters()
        networkInfo =  {'bytesSend': net.bytes_sent/1024/1024/1024,
                        'bytesRecv': net.bytes_recv/1024/1024/1024,
                       }
        return networkInfo
   
    def db_info(self):
        ''''''
        cmd = '''ps auxww|grep mysqld|grep -v root|grep -v grep \
                   | awk -F " " '{print $NF}'| grep port|awk -F "=" '{print $2}\''''
        dbInfo = {'mysql_port': commands.getoutput(cmd),}
        return dbInfo
   
    def process_info(self):
        '''Total number of running processes'''
        pids = []
        for subdir in os.listdir('/proc'):
            if subdir.isdigit():
                pids.append(subdir)
   
        processInfo = {'total_process': len(pids),}
        return processInfo
   
    def load_info(self):
        ''''''
        f = open("/proc/loadavg")
        con = f.read().split()
        f.close()
   
        loadInfo = {'lavg_1': con[0],
                'lavg_5': con[1],
                'lavg_15': con[2],
              }    
        return loadInfo    

    def result_info(self):
        result = []
        data_sets={
            "base": self.base_info,
            'disk': self.disk_info,
            'memory': self.memory_info,
            'network': self.network_info,
            'db': self.db_info,
            'process': self.process_info,
            'load': self.load_info
        }
        for _key in data_sets.keys():
            _d = data_sets.get(_key)()
            if _d :
                result.append({_key:_d})
        return result

def main():
    ''''''
    b=hostInfo()
    print b.result_info()

if __name__ == "__main__":
    main()

