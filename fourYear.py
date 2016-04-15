# -*- coding: utf-8 -*-
from __future__ import division
from statistics import Statistics 
import logging
import pymongo
import os
import sys


class FourYear(Statistics):
    def __init__(self, dbName1, dbName2, rootDir = None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        self.total = self.table1.find({'ji': {'$ne': ""}}).count()
        #self.total = Statistics.numberOfGongLiNianEqual(self)
        self.rootDir = os.path.join(self.rootDir, u"步骤三数据库数据的匹配程度")
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"2.不考虑公历年")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"3.考虑公历年")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"4.连续4年+3") 
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)    
            
        self.logger = logging.getLogger('TwoYear')
        self.logger.info("totol record: {}".format(self.total))
            
    def step1(self):
        resultDir = self.createDir(str(1))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            gongLiNian = int(element['gongLiNian'])
            gongLiNianScope = [str(gongLiNian+3), str(gongLiNian+2), str(gongLiNian+1), str(gongLiNian)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'name'    :  element['name']})
            if res:
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
            gongLiNianScope = [str(gongLiNian+3), str(gongLiNian+2), str(gongLiNian+1), str(gongLiNian)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'],
                                        'name'    : element['name'],
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'minZu'   : element['minZu']})
            if res:
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
            gongLiNianScope = [str(gongLiNian+3), str(gongLiNian+2), str(gongLiNian+1), str(gongLiNian)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'name'    : element['name'],
                                        'qiFen'   : element['qiFen'],
                                        'minZu'   : element['minZu']})
        
            if res:
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
            gongLiNianScope = [str(gongLiNian+3), str(gongLiNian+2), str(gongLiNian+1), str(gongLiNian)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'name'    : element['name'],
                                        'qiFen'   : element['qiFen'],
                                        'keJu'    : element['keJu'],
                                        'minZu'   : element['minZu']})
        
            if res:
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
            gongLiNianScope = [str(gongLiNian+3), str(gongLiNian+2), str(gongLiNian+1), str(gongLiNian)]
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                        'name'    : element['name'],
                                     'gongLiNian' : {'$in': gongLiNianScope},
                                        'keJu'    : element['keJu']})
            if res :
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
    fourYear = FourYear('table1', 'table2')
    fourYear.run()