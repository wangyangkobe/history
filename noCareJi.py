# -*- coding: utf-8 -*-
from __future__ import division
from statistics import Statistics 
import logging
import os

class OneJi(Statistics):
    def __init__(self, dbName1, dbName2, rootDir = None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        self.total = self.table1.count()
        self.rootDir = os.path.join(self.rootDir, u"步骤三数据库数据的匹配程度")
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"2.不考虑公历年")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"1.不考虑季节")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
                
        self.logger = logging.getLogger(u'不考虑季')
         
    def step1(self):
        resultDir = self.createDir("1")
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find():
            if self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'name'    : element['name']}):
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        result = (success, success/self.total, failed, failed/self.total)    
        self.logResult(1, result) 
        return result
    
    def step2(self):
        resultDir = self.createDir("2")
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find():
            if self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'name'    : element['name'],
                                     'minZu'   : element['minZu']}):
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        result = (success, success/self.total, failed, failed/self.total)    
        self.logResult(2, result) 
        return result
    
    def step3(self):
        resultDir = self.createDir("3")
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find():
            result = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                           'name'    : element['name'],
                                           'minZu'   : element['minZu']})
        
            if result and (element['qiFen'] == result['qiFen'] or 
                           element['qiFen'] == result['qiFenHuo']):
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        result = (success, success/self.total, failed, failed/self.total)    
        self.logResult(3, result) 
        return result

    def step4(self):
        resultDir = self.createDir("4")
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find():
            result = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                           'name'    : element['name'],
                                           'keJu'    : element['keJu'],
                                           'minZu'   : element['minZu']})
        
            if result and (element['qiFen'] == result['qiFen'] or 
                           element['qiFen'] == result['qiFenHuo']):
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        result = (success, success/self.total, failed, failed/self.total)    
        self.logResult(4, result) 
        return result
    
    def step5(self):
        resultDir = self.createDir("5")
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find():
            result = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                           'name'    : element['name'],
                                           'keJu'    : element['keJu']})
        
            if result:
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
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