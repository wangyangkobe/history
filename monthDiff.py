# -*- coding: utf-8 -*-
from __future__ import division
from statistics import Statistics 
import logging
import os
import pymongo
import lib

dbClient = pymongo.MongoClient()
db1 = dbClient.history["table1"]
db2 = dbClient.history["table2"]

def scope(ji):
    if ji == '1':
        return (1,3)
    elif ji == '2':
        return (4,6)
    elif ji == '3':
        return (7,9)
    else:
        return (10,12)

def compare(db1Lines, db2Line):
    tmp = []
    for line in db1Lines:
        tmp.append( (line, int(line['ji'])) )
    tmp.sort(key=lambda ele: ele[1])
    db1Line = tmp[0][0]
    
    (left, right) = scope(db1Line['ji'])
    month = int(db2Line['ji'])
    if month < left:
        return month - left #提前
    elif month > right:
        return month - right #延后
    else:
        return 0
    
           
def worker(rootDir, month):
    matched    = []
    no_matched = []
    if month < 0:
        dirName = "提前{}个月".format(abs(month))
    elif month > 0:
        dirName = "延后{}个月".format(abs(month))
    else:
        dirName = "同一季".format(abs(month))

    total = 0
    for r2 in db2.find({'ji': {'$ne': ""}}):
        #r1 = db1.find({"name":r2["name"], "guanZhi":r2['guanZhi'], "gongLiNian":r2['gongLiNian']})
        r1 = db1.find({"name":r2["name"], "guanZhi":r2['guanZhi']})
        if r1.count() > 0:
            total += 1
            if compare(r1, r2) == month:
                matched.append(r2)
            else:
                no_matched.append(r2)
        else:
            pass
    
    rootDir = os.path.join(rootDir, dirName)
    if not os.path.exists(rootDir):
        os.mkdir(rootDir)
    
    with open(os.path.join(rootDir, 'match.txt'), "w") as f_:
        lib.writeDataForTable2(f_, matched)
    with open(os.path.join(rootDir, 'no_match.txt'), "w") as f_:
        lib.writeDataForTable2(f_, no_matched)
        
    with open(os.path.join(rootDir, 'result.txt'), 'w') as f:
        print 'match: {}, not_match: {}, rate: {}'.format(len(matched), len(no_matched), len(matched)/total)
        f.write('match: {}, not_match: {}, rate: {}\n'.format(len(matched), len(no_matched), len(matched)/total))
    print "{} Done!!!\n".format(dirName)
    
    
if __name__ == '__main__':
    rootDir = os.path.join(os.path.dirname(__file__), u"步骤二季的差异问题")
    if not os.path.exists(rootDir):
        os.mkdir(rootDir)
    for i in [-9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
        worker(rootDir, i)
    print "Done!!!"