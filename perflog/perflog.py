__author__ = 'sherry.zheng'

import os
import subprocess
import time


def monitorLog(logfile):

    popen = subprocess.Popen('tail -f /opt/smartprobe/var/run/report/dp/' + logfile,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, shell=True)
    while True:
        line = popen.stdout.readline()
        if line:
            print line


def monitorShm(shmfile):
    shm = os.popen('/opt/smartprobe/sp/pktm/v1/util/pktmonr -e shm://' + shmfile)
    while True:
        line = shm.readline()
        if line:
            print os.system("awk -F, '{print $4}' %s" % line)
    #while True:
        #line = shm.stdout.readline()
        #if line:
            #op = line.split(',')
            #print os.system("awk -F, '{print $4}' %s" % line)

if __name__ == "__main__":
    #monitorLog("dp_default-cache.log")
    monitorShm('nic0')
