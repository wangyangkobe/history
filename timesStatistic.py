# -*- coding: utf-8 -*-
from __future__ import division
from statistics import Statistics 
import logging
import os
import lib

if __name__ == '__main__':
    rootDir = os.path.join(os.path.dirname(__file__), u'步骤三数据库数据的匹配程度')
    if not os.path.exists(rootDir):
        os.mkdir(rootDir)
    rootDir = os.path.join(rootDir, u'1.同一官职的任职次数和科举出身的变化')
    if not os.path.exists(rootDir):
        os.mkdir(rootDir)
    
    result = set()
    for ele in lib.table1.find():
        times = set()
        for ele2 in lib.table1.find({'name': ele['name'], 'guanZhi': ele['guanZhi']}):
            times.add(ele2['gongLiNian'])
        result.add(( ele['ming'], ele['guanZhi'], len(times)))
    with open(os.path.join(rootDir, '数据库1公历年的次数.txt'), "w") as f:
        f.write("姓名;官职;次数\n")
        for ele in result:
            f.write("{};{};{}\n".format(ele[0], ele[1], ele[2]))
    print "finish 数据库1公历年的次数, total: {}".format(len(result))
    
    result = set()
    for ele in lib.table2.find():
        times = set()
        for ele2 in lib.table2.find({'name': ele['name'], 'guanZhi': ele['guanZhi']}):
            times.add(ele2['gongLiNian'])
        result.add(( ele['ming'], ele['guanZhi'], len(times)))
    with open(os.path.join(rootDir, '数据库2公历年的次数.txt'), "w") as f:
        f.write("姓名;官职;次数\n")
        for ele in result:
            f.write("{};{};{}\n".format(ele[0], ele[1], ele[2]))
    print "finish 数据库2公历年的次数, total: {}".format(len(result))
    
    result = set()
    for ele in lib.table1.find():
        times = set()
        for ele2 in lib.table1.find({'name': ele['name']}):
            times.add(ele2['keJu'])
        if len(times) > 1:
            result.add(( ele['ming'], ','.join(times), len(times)))
    with open(os.path.join(rootDir, '数据库1科举的次数.txt'), "w") as f:
        f.write("姓名;科举;次数\n")
        for ele in result:
            f.write("{};{};{}\n".format(ele[0], ele[1], ele[2]))
    print "finish 数据库1科举的次数, total: {}".format(len(result))
    
                
    