# -*- coding: utf-8 -*-
from __future__ import division 
import logging
import pymongo
import os
import sys
import lib

reload(sys)
sys.setdefaultencoding("utf-8")

class Statistics:
    dbClient = pymongo.MongoClient()
    logging.basicConfig(level=logging.INFO)
    def __init__(self, dbName1, dbName2, rootDir):
        if rootDir:
            self.rootDir = rootDir
        else:
            self.rootDir = os.path.dirname(__file__)
        self.table1 = Statistics.dbClient.history[dbName1]
        self.table2 = Statistics.dbClient.history[dbName2]
        self.logger = logging.getLogger('Statistics')
    
    def convertJi(self, month):
        if month in ["1", "2", "3"]:
            return 1
        elif month in ["4", "5", "6"]:
            return 2
        elif month in ["7", "8", "9"]:
            return 3
        else:
            return 4
        
    def createDir(self, dirName):
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        resultDir = os.path.join(self.rootDir, dirName)
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        return resultDir
    
    def createResultFile(self, dirName): 
        self.logger.info("{} {}".format("createResultFile", dirName))
        matched    = open(os.path.join(dirName, 'match.txt'),    "w")
        notMatched = open(os.path.join(dirName, 'no_match.txt'), "w")
    
        matched.write(lib.headers1 + '\n')
        notMatched.write(lib.headers1 + '\n')
    
        return (matched, notMatched)
    
    def formatElement(self, element):
        res = []
        for key in lib.keys1:
            res.append(element[key])  
        return ';'.join(res) + '\n'
    
    def formatElement2(self, element):
        res = []
        for key in lib.keys2:
            res.append(element[key])  
        return ';'.join(res) + '\n'
    
    def formatElement3(self, element):
        res = []
        for key in lib.keys3:
            res.append(element[key])  
        return ';'.join(res) + '\n'
    
    def logResult(self, step, result):
        titles = [u"", u"1:官职+姓名", u"2:官职+姓名+民族", u"3:官职+姓名+民族+旗分", u"4官职+姓名+民族+旗分+科举", u"5官职+姓名+科举"]
        self.logger.info("step{} --- success: {} ration: {} failed: {} ratio: {}"
                         .format(titles[step], result[0], result[1], result[2], result[3]))
    
    def numberOfGongLiNianEqual(self):
        years = []
        set2 = set()
        set3 = set()
        
        for element in self.table2.find():
            if len(element['ji']) > 0:
                set2.add(element['gongLiNian'])
            else:
                set3.add(element['gongLiNian'])
    
        i = 0
        j = 0
        k = 0         
        for element in self.table1.find():
            if element['gongLiNian'] in set2:
                i += 1
                years.append(element['gongLiNian'])
            else:
                j += 1
                
            if element['gongLiNian'] in set3:
                k += 1
        print i, j, k    
        return len(years)

