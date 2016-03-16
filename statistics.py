# -*- coding: utf-8 -*-
from __future__ import division 
import logging
import pymongo
import os
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

class Statistics:
    headers  = r"季节;朝代;年;公历年;季;衙门;官职;官职简写;官员;品级;身份;名;民族;旗分;爵位;科举"
    keys     = ['jiJie', 'chaoDai', 'nian', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'minZu', 'qiFen', 'jueWei', 'keJu']
    headers2 = r"朝代;年;月;公历年;季;衙门;官职;官职简写;官员;品级;身份;名;又名;民族;旗分;旗分（或）;爵位;科举;科举（其他）"
    keys2    = ['chaoDai', 'nian', 'yue', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'youMing', 'minZu', 'qiFen', 'qiFenHuo', 'jueWei', 'keJu', 'keJuHuo']
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
    
        matched.write(Statistics.headers + '\n')
        notMatched.write(Statistics.headers + '\n')
    
        return (matched, notMatched)
    def formatElement(self, element):
        res = []
        for key in Statistics.keys:
            res.append(element[key])  
        return ';'.join(res) + '\n'
    
    def formatElement2(self, element):
        res = []
        for key in Statistics.keys2:
            res.append(element[key])  
        return ';'.join(res) + '\n'
    
    def logResult(self, step, result):
        self.logger.info("step{} --- success: {} ration: {} failed: {} ratio: {}"
                         .format(step, result[0], result[1], result[2], result[3]))
