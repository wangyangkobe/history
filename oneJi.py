# -*- coding: utf-8 -*-
from __future__ import division
from statistics import Statistics 
import logging
import pymongo
import os
import sys


class OneJi(Statistics):
    def __init__(self, dbName1, dbName2, rootDir = None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        #self.total = self.table1.find({'ji': {'$ne': ""}}).count()
        self.total = Statistics.numberOfGongLiNianEqual(self)
        self.rootDir = os.path.join(self.rootDir, u"步骤三数据库数据的匹配程度")
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"2.不考虑公历年")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"2.考虑季节")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"2.同一季")  
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)    
            
        self.logger = logging.getLogger('OneJi')
        self.logger.info("totol record: {}".format(self.total))
            
    def step1(self):
        resultDir = self.createDir(str(1))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            gongLiNian = int(element['gongLiNian'])
            gongLiNianScope = [str(gongLiNian-1), str(gongLiNian), str(gongLiNian+1)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'name'    :  element['name']})
            if res and (len(res['ji']) > 0) and (Statistics.convertJi(self, res['ji']) == int(element['ji'])):
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        failed = self.total - success
        result = (success, success/self.total, failed, failed/self.total)    
        self.logResult(1, result) 
        return result
    
    def step2(self):
        resultDir = self.createDir(str(2))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            gongLiNian = int(element['gongLiNian'])
            gongLiNianScope = [str(gongLiNian-1), str(gongLiNian), str(gongLiNian+1)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'],
                                        'name'    : element['name'],
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'minZu'   : element['minZu']})
            if res and (len(res['ji']) > 0) and (Statistics.convertJi(self, res['ji']) == int(element['ji'])):
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        failed = self.total - success
        result = (success, success/self.total, failed, failed/self.total)    
        self.logResult(2, result) 
        return result
    def step3(self):
        resultDir = self.createDir(str(3))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            gongLiNian = int(element['gongLiNian'])
            gongLiNianScope = [str(gongLiNian-1), str(gongLiNian), str(gongLiNian+1)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'name'    : element['name'],
                                        'minZu'   : element['minZu']})
        
            if res and (len(res['ji']) > 0) and (Statistics.convertJi(self, res['ji']) == int(element['ji'])) and (element['qiFen'] == res['qiFen'] or element['qiFen'] == res['qiFenHuo']):
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        failed = self.total - success
        result = (success, success/self.total, failed, failed/self.total)    
        self.logResult(3, result) 
        return result

    def step4(self):
        resultDir = self.createDir(str(4))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            gongLiNian = int(element['gongLiNian'])
            gongLiNianScope = [str(gongLiNian-1), str(gongLiNian), str(gongLiNian+1)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'name'    : element['name'],
                                        'keJu'    : element['keJu'],
                                        'minZu'   : element['minZu']})
        
            if res and (len(res['ji']) > 0) and (Statistics.convertJi(self, res['ji']) == int(element['ji'])) and (element['qiFen'] == res['qiFen'] or element['qiFen'] == res['qiFenHuo']):
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        failed = self.total - success
        result = (success, success/self.total, failed, failed/self.total)    
        self.logResult(4, result) 
        return result
    
    def step5(self):
        resultDir = self.createDir(str(5))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            gongLiNian = int(element['gongLiNian'])
            gongLiNianScope = [str(gongLiNian-1), str(gongLiNian), str(gongLiNian+1)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                        'name'    : element['name'],
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'keJu'    : element['keJu']})
            
        
            if res and (len(res['ji']) > 0) and (Statistics.convertJi(self, res['ji']) == int(element['ji'])):
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        failed = self.total - success
        result = (success, success/self.total, failed, failed/self.total)    
        self.logResult(5, result) 
        return result
        
    def run(self):
        res1 = self.step1()
        res2 = self.step2()
        res3 = self.step3()
        res4 = self.step4()
        res5 = self.step5()
        resultFile = open(os.path.join(self.rootDir, 'result.txt'), "w")
        index = 0
        titles = [ u"1:官职+姓名", u"2:官职+姓名+民族", u"3:官职+姓名+民族+旗分", u"4:官职+姓名+民族+旗分+科举", u"5:官职+姓名+科举"]
        for (a, b, c, d) in [res1, res2, res3, res4, res5]:
            resultFile.write("step{}: {} {} {} {}\n".format(titles[index], a, b, c, d))
            index += 1

if __name__ == '__main__':
    oneJi = OneJi('table1', 'table2')
    oneJi.run()
































reload(sys)
sys.setdefaultencoding("utf-8")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('OneJi')

dbClient = pymongo.MongoClient()
db = dbClient.history
table1 = db.table1
table2 = db.table2

total = db.table1.count()
dirPath =os.path.join(os.path.dirname(__file__), u'同一季')

header  = r"季节;朝代;年;公历年;季;衙门;官职;官职简写;官员;品级;身份;名;民族;旗分;爵位;科举"
header1 = ['jiJie', 'chaoDai', 'nian', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'minZu', 'qiFen', 'jueWei', 'keJu']

def createResultFile(dirName):
    logger.info("{} {}".format("createResultFile", dirName))
    matched    = open(os.path.join(dirName, 'match.txt'),    "w")
    notMatched = open(os.path.join(dirName, 'no_match.txt'), "w")
    
    matched.write(header + '\n')
    notMatched.write(header + '\n')
    
    return (matched, notMatched)
 
def formatElement(element):
    res = []
    for key in header1:
        res.append(element[key])  
    return ';'.join(res) + '\n'

def createDir(name):
    if not os.path.exists(dirPath):
        os.mkdir(dirPath)
    resultDir = os.path.join(dirPath, name)
    if not os.path.exists(resultDir):
        os.mkdir(resultDir)
    return resultDir
          
def step1():
    resultDir = createDir(str(1))
    (match, no_match) = createResultFile(resultDir)
    
    success = 0
    failed  = 0    
    for element in table1.find({'ji': {"$ne" : ""}}):
        if table2.find_one({'guanZhi' : element['guanZhi'],
                            'name'    : element['name']}):
            match.write(formatElement(element))
            success += 1 
        else:
            no_match.write(formatElement(element))
            failed += 1
    logger.info("step1 --- success: {} ratio: {} failed: {} ratio: {}".format(success, success/total, failed, failed/total))
    return (success, success/total, failed, failed/total)

def step2():
    resultDir = createDir(str(2))
    (match, no_match) = createResultFile(resultDir)
    
    success = 0
    failed = 0    
    for element in table1.find({'ji': {"$ne" : ""}}):
        if table2.find_one({'guanZhi' : element['guanZhi'],
                            'name'    : element['name'],
                            'minZu'   : element['minZu']}):
            success += 1
            match.write(formatElement(element))
        else:
            no_match.write(formatElement(element))
            failed += 1
    logger.info("step2 --- success: {} ratio: {} failed: {} ratio: {}".format(success, success/total, failed, failed/total))
    return (success, success/total, failed, failed/total)

def step3():
    resultDir = createDir(str(3))
    (match, no_match) = createResultFile(resultDir)
    
    success = 0
    failed  = 0    
    for element in table1.find({'ji': {"$ne" : ""}}):
        result = table2.find_one({'guanZhi' : element['guanZhi'], 
                                  'name'    : element['name'],
                                  'minZu'   : element['minZu']})
        
        if result and (element['qiFen'] == result['qiFen'] or 
                       element['qiFen'] == result['qiFenHuo']):
            success += 1
            match.write(formatElement(element))
        else:
            no_match.write(formatElement(element))
            failed += 1
    logger.info("step3 --- success: {} ratio: {} failed: {} ratio: {}".format(success, success/total, failed, failed/total))
    return (success, success/total, failed, failed/total)

def step4():
    resultDir = createDir(str(4))
    (match, no_match) = createResultFile(resultDir)
    
    success = 0    
    failed  = 0    
    for element in table1.find({'ji': {"$ne" : ""}}):
        result =  table2.find_one({'guanZhi' : element['guanZhi'],
                                   'name'    : element['name'],
                                   'minZu'   : element['minZu'],
                                   'keJu'    : element['keJu']})
        if result and (element['qiFen'] == result['qiFen'] or 
                       element['qiFen'] == result['qiFenHuo']):
            match.write(formatElement(element))
            success += 1
        else:
            no_match.write(formatElement(element))
            failed += 1
    logger.info("step4 --- success: {} ratio: {} failed: {} ratio: {}".format(success, success/total, failed, failed/total))
    return (success, success/total, failed, failed/total)

def run():
    res1 = step1()
    res2 = step2()
    res3 = step3()
    res4 = step4()
    resultFile = open(os.path.join(dirPath, 'result.txt'), "w")
    index = 1
    for (a, b, c, d) in [res1, res2, res3, res4]:
        resultFile.write("step{}: {} {} {} {}\n".format(index, a, b, c, d))
        index += 1
    
if __name__ == '__main__':
    pass
    #run()