# -*- coding: utf-8 -*-
from __future__ import division
from statistics import Statistics 
import logging
import os
import lib
import itertools

if __name__ == '__main__':
    rootDir = os.path.join(os.path.dirname(__file__), u'步骤三数据库数据的匹配程度')
    if not os.path.exists(rootDir):
        os.mkdir(rootDir)
    rootDir = os.path.join(rootDir, u'1.同一官职的任职次数和科举出身的变化')
    if not os.path.exists(rootDir):
        os.mkdir(rootDir)
    
    db1Lines = []
    for ele in lib.table1.find():
        db1Lines.append(ele)
    db1Lines.sort(key = lambda element : (element['ming'], element['guanZhi']))
    f = open(os.path.join(rootDir, '数据库1公历年的次数.txt'), "w")
    f.write("季节;朝代;年;公历年;季;衙门;官职;官职简写;官员;品级;身份;名;公历年次数;原姓名;民族;旗分;爵位;科举\n")
    keys = ['jiJie', 'chaoDai', 'nian', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'times', 'yuanXingMing', 'minZu', 'qiFen', 'jueWei', 'keJu']
    for _, values in itertools.groupby(db1Lines, key = lambda element : (element['ming'], element['guanZhi'])):
        tmp = [x for x in values]
        for value in tmp:
            value['times'] = str(len(tmp))
            line = []
            for key in keys:
                line.append(value[key])
            f.write(';'.join(line) + '\n')
    f.close()
    print "finish 数据库1科举的次数, total: {}".format(len(db1Lines))
    
    db2Lines = []
    for ele in lib.table2.find():
        db2Lines.append(ele)
    db2Lines.sort(key = lambda element : (element['ming'], element['guanZhi']))
    f = open(os.path.join(rootDir, '数据库2公历年的次数.txt'), "w")
    f.write("朝代;年;月;公历年;季;衙门;官职;官职简写;官员;品级;身份;名;公历年次数;又名;民族;旗分;旗分（或）;爵位;科举;科举（其他）\n")
    keys = ['chaoDai', 'nian', 'yue', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'times', 'youMing', 'minZu', 'qiFen', 'qiFenHuo', 'jueWei', 'keJu', 'keJuHuo']
    for _, values in itertools.groupby(db2Lines, key = lambda element : (element['ming'], element['guanZhi'])):
        tmp = [x for x in values]
        for value in tmp:
            value['times'] = str(len(tmp))
            line = []
            for key in keys:
                line.append(value[key])
            f.write(';'.join(line) + '\n')
    f.close()
    print "finish 数据库2科举的次数, total: {}".format(len(db2Lines))
    #===========================================================================
    # result = set()
    # for ele in lib.table2.find():
    #     times = set()
    #     for ele2 in lib.table2.find({'name': ele['name'], 'guanZhi': ele['guanZhi']}):
    #         times.add(ele2['gongLiNian'])
    #     result.add(( ele['ming'], ele['guanZhi'], len(times)))
    # with open(os.path.join(rootDir, '数据库2公历年的次数.txt'), "w") as f:
    #     f.write("姓名;官职;次数\n")
    #     for ele in result:
    #         f.write("{};{};{}\n".format(ele[0], ele[1], ele[2]))
    # print "finish 数据库2公历年的次数, total: {}".format(len(result))
    #===========================================================================
    
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
    
                
    