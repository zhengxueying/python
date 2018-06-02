#!/usr/bin/env python
#-*- coding: utf-8 -*-
import os
import json
import shlex
import time
from datetime import datetime,date,timedelta
import shutil

import paramiko
from paramiko import SSHException
from subprocess import Popen, PIPE
import random


class MockMongo(object):

    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def __init__(self):
        # cur_path = os.path.abspath('.')
        # self.dir = os.path.abspath(os.path.join(os.getcwd(),"../../../../Resource/fixture/mock_mongo"))

        self.dir = os.path.abspath(os.path.join(os.getcwd(),"../Resource/fixture/mock_mongo"))
        # print self.dir

    def handle_file(self,file, ts):
        f = open(file,'r')
        dict_list = []
        # print file
        f_data = f.readlines()
        for data in f_data:
            # print data
            dic = json.loads(data)
            dic['ts'] = ts
            dict_list.append(dic)
        f.close()
        return dict_list
    # def clean_dir(self, path):
    #mock baseline:baseline_log collection
    def change_metric(self,file,type):
        with open(file,'r') as f:
            # f_data = f.readline()
            # print f_data
            dic = json.load(f)
            data = dic['data']
            for item in data:
                item['bit_rate'] = 10
        with open(file,'w') as dump_f:
            json.dump(dic,dump_f)
        f.close()
        return self

    def baseline_config(self,file):
        with open(file,'r') as load_f:
            f_data = load_f.readlines()
            dump_f = open('F:\\json\\dump.json', 'a')
            yest = date.today() - timedelta(-1)
            # print today
            yest_ts = time.mktime(yest.timetuple())
            # print today_ts
            for data in f_data:
                dic = json.loads(data)     #1 of task
                for a in dic['algorithm']:
                    value = random.randint(1,100)
                    if dic['granularity'] == '1m':
                        f_1m = open("F:\\json\\baseline_1m_tpl.json",'r')
                        dic_1m = json.load(f_1m)   #tpl data
                        dic_1m['hash_id'] = dic['hash_id']
                        dic_1m['config_id'] = dic['_id']
                        #算法和数据插入方式怎么搞？
                        metric = dic['metric']
                        keys = dic_1m['data'][0].keys()
                        for k in xrange(len(keys)):
                            if keys[k] == 'ts':
                                keys.pop(k)
                        for data in dic_1m['data']:
                            data[metric] = data.pop('%s'%keys[0])
                            data[metric] = value
                            # print data
                        dic_1m['algorithm'] = a
                        dic_1m['day_ts'] = int(yest_ts)
                        print dic_1m
                        dump_f.write(json.dumps(dic_1m))
                    elif dic['granularity'] == '5m':
                        f_1m = open("F:\\json\\baseline_5m_tpl.json",'r')
                        dic_1m = json.load(f_1m)   #tpl data
                        dic_1m['hash_id'] = dic['hash_id']
                        dic_1m['config_id'] = dic['_id']
                        #算法和数据插入方式怎么搞？
                        metric = dic['metric']
                        keys = dic_1m['data'][0].keys()
                        for k in xrange(len(keys)):
                            if keys[k] == 'ts':
                                keys.pop(k)
                        for data in dic_1m['data']:
                            data[metric] = data.pop('%s'%keys[0])
                            data[metric] = value
                            # print data
                        dic_1m['algorithm'] = a
                        dic_1m['day_ts'] = int(yest_ts)
                        print dic_1m
                        dump_f.write(json.dumps(dic_1m))
                    elif dic['granularity'] == '15m':
                        f_1m = open("F:\\json\\baseline_15m_tpl.json",'r')
                        dic_1m = json.load(f_1m)   #tpl data
                        dic_1m['hash_id'] = dic['hash_id']
                        dic_1m['config_id'] = dic['_id']
                        #算法和数据插入方式怎么搞？
                        metric = dic['metric']
                        keys = dic_1m['data'][0].keys()
                        for k in xrange(len(keys)):
                            if keys[k] == 'ts':
                                keys.pop(k)
                        for data in dic_1m['data']:
                            data[metric] = data.pop('%s'%keys[0])
                            data[metric] = value
                            # print data
                        dic_1m['algorithm'] = a
                        dic_1m['day_ts'] = int(yest_ts)
                        print dic_1m
                        dump_f.write(json.dumps(dic_1m))
                    elif dic['granularity'] == '1h':
                        f_1m = open("F:\\json\\baseline_1h_tpl.json",'r')
                        dic_1m = json.load(f_1m)   #tpl data
                        dic_1m['hash_id'] = dic['hash_id']
                        dic_1m['config_id'] = dic['_id']
                        #算法和数据插入方式怎么搞？
                        metric = dic['metric']
                        keys = dic_1m['data'][0].keys()
                        for k in xrange(len(keys)):
                            if keys[k] == 'ts':
                                keys.pop(k)
                        for data in dic_1m['data']:
                            data[metric] = data.pop('%s'%keys[0])
                            data[metric] = value
                            # print data
                        dic_1m['algorithm'] = a
                        dic_1m['day_ts'] = int(yest_ts)
                        print dic_1m
                        dump_f.write(json.dumps(dic_1m))
                    else:
                        return
            dump_f.close()

        load_f.close()
        return self
    def line_type(self,type):
        if type == 'horizontal':
            value = random.randint(1,100)
        elif type == "asc_line":
            pass
        elif type == "desc_line":
            pass
        else:
            pass

    def change_ts(self, sttime, num):
        # self._ssh_connect(host)
        st = datetime.strptime(sttime,'%Y-%m-%d %H:%M:%S')
        ts = time.mktime(st.timetuple())
        json_data = []
        mock_path = os.path.join(self.dir, 'mongoimp')
        if os.path.exists(mock_path):
            shutil.rmtree(mock_path)
        os.mkdir(mock_path)
        print str(os.path.join(self.dir, 'mongo_ts'))
        for file in self.getfiles(os.path.join(self.dir, 'mongo_ts')):
            print file
            cur_ts = ts
            print cur_ts
            fname = os.path.split(file)[1]
            for i in xrange(int(num)):
                f = open(os.path.join(mock_path, fname), 'a')
                # print cur_ts
                for dic in self.handle_file(file, cur_ts):
                    f.write(json.dumps(dic))
                f.close()
                cur_ts += 3600
        return self

    def change_spv_site_ts(self, sttime, num):
        # self._ssh_connect(host)
        st = datetime.strptime(sttime,'%Y-%m-%d %H:%M:%S')
        ts = time.mktime(st.timetuple())
        json_data = []
        mock_path = os.path.join(self.dir, 'mongoimp_spv_site')
        if os.path.exists(mock_path):
            shutil.rmtree(mock_path)
        os.mkdir(mock_path)
        print str(os.path.join(self.dir, 'mongo_spv_site'))
        for file in self.getfiles(os.path.join(self.dir, 'mongo_spv_site')):
            print file
            cur_ts = ts
            fname = os.path.split(file)[1]
            for i in xrange(int(num)):
                f = open(os.path.join(mock_path, fname), 'a')
                # print cur_ts
                for dic in self.handle_file(file, cur_ts):
                    f.write(json.dumps(dic))
                f.close()
                cur_ts += 3600
        return self
    def change_spv_ts(self, sttime, num):
        # self._ssh_connect(host)
        st = datetime.strptime(sttime,'%Y-%m-%d %H:%M:%S')
        ts = time.mktime(st.timetuple())
        json_data = []
        mock_path = os.path.join(self.dir, 'mongoimp_spv')
        if os.path.exists(mock_path):
            shutil.rmtree(mock_path)
        os.mkdir(mock_path)
        print str(os.path.join(self.dir, 'mongo_spv'))
        for file in self.getfiles(os.path.join(self.dir, 'mongo_spv')):
            print file
            cur_ts = ts
            fname = os.path.split(file)[1]
            for i in xrange(int(num)):
                f = open(os.path.join(mock_path, fname), 'a')
                # print cur_ts
                for dic in self.handle_file(file, cur_ts):
                    f.write(json.dumps(dic))
                f.close()
                cur_ts += 3600
        return self

    def getfiles(self, dir):
        files = []
        f = os.walk(dir)
        for path, dirs, filelist in f:
            for filename in filelist:
                files.append(os.path.join(path, filename))
        return files

    def rm_dir(self,dir):
        files = self.getfiles(dir)
        for f in files:
            if os.path.exists(f):
                os.remove(f)
        return self

    def mongo_imp(self, host, spid=None, eth=None, sttime=None):
        path = os.path.join(self.dir, 'mongoimp')
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=host, username='root', password='rootroot', timeout=5, allow_agent=False,
                         look_for_keys=False)
        for file in self.getfiles(path):
            if spid != None and eth != None and sttime != None:
                dbname = spid + '_' + eth + '_1m_' + sttime
                coll = os.path.basename(file).split('.')[0].split('-')[1]
                print dbname
            else:
                filename = os.path.basename(file).split('.')[0]
                dbname = filename.split('-')[0]
                coll = filename.split('-')[1]
                print dbname,coll
            cmd = "mongoimport -h %s -d %s -c %s %s" % (host, dbname, coll, '/root/mongoimp/'+str(os.path.basename(file)))
            print cmd
            self.ssh.exec_command(cmd)
        return self

    def mongo_imp_spv_site(self, host, spvid=None, sttime=None):
        path = os.path.join(self.dir, 'mongoimp_spv_site')
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=host, username='root', password='rootroot', timeout=5, allow_agent=False,
                         look_for_keys=False)
        for file in self.getfiles(path):
            dbname = 'npm_data_' + spvid + '_1m_' + sttime
            coll = os.path.basename(file).split('.')[0]
                # print dbname,coll
            cmd = "mongoimport -d %s -c %s %s" % (dbname, coll, '/root/mongoimp_spv_site/'+str(os.path.basename(file)))
            print cmd
            self.ssh.exec_command(cmd)
        return self
    def mongo_imp_spv(self, host, spvid=None, cap=None, sttime=None):
        path = os.path.join(self.dir, 'mongoimp_spv')
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=host, username='root', password='rootroot', timeout=5, allow_agent=False,
                         look_for_keys=False)
        for file in self.getfiles(path):
            dbname = 'npm_data_' + spvid + '_1m_' + sttime
            collname = os.path.basename(file).split('.')[0]
                # print dbname,coll
            capname = collname.split('_')
            capname[1] = cap
            coll = '_'.join(capname)
            print coll
            cmd = "mongoimport -d %s -c %s %s" % (dbname, coll, '/root/mongoimp_spv/'+str(os.path.basename(file)))
            print cmd
            self.ssh.exec_command(cmd)
        return self
    def date_cvt(self,time):
        t = time.split('-')
        return ''.join(t[:2])

if __name__ == '__main__':
    # MockMongo().mongo_imp('10.1.1.131','361264973d81','eth2','201805')
    # MockMongo().mongo_imp_spv('10.1.1.131', 'spv1', '201804')
    # MockMongo().change_ts("F:\\robot\\Resource\\fixture\\mock_mongo\\", '2018-05-18 10:00:00', 3)
    # MockMongo().change_spv_ts('2018-05-18 10:00:00', 3)
    # MockMongo().change_metric("F:\\json\\baseline.json",None)
    MockMongo().baseline_config("F:\\json\\baseline_task.json")