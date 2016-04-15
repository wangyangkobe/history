# -*- coding: utf-8 -*-
from __future__ import division
from statistics import Statistics 
import logging
import os

class ThreeJi(Statistics):
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
        
        self.rootDir = os.path.join(self.rootDir, u"ͬ4.连续3季")
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)  
            
        self.logger = logging.getLogger('ThreeJi')
    
    def jiScope(self, ji):
        if ji == "1":
            return [3, 4, 1, 2, 3]
        elif ji == "2":
            return [4, 1, 2, 3, 4]
        elif ji == "3":
            return [1, 2, 3, 4, 1]
        else:
            return [2, 3, 4, 1, 2]    
    def step1(self):
        resultDir = self.createDir(str(1))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            ji = element['ji']
            res = self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'gongLiNian' : element['gongLiNian'],
                                        'name'    : element['name']})
            if res and (len(ji) > 0) and (Statistics.convertJi(self, res['ji']) in self.jiScope(ji)):
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
            ji = element['ji']
            res = self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'gongLiNian' : element['gongLiNian'],
                                        'name'    : element['name'],
                                        'minZu'   : element['minZu']})
            if res and (len(ji) > 0) and (Statistics.convertJi(self, res['ji']) in self.jiScope(ji)):
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
            ji = element['ji']
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                     'gongLiNian' : element['gongLiNian'],
                                        'name'    : element['name'],
                                        'minZu'   : element['minZu']})
        
            if res and (len(ji) > 0) and (Statistics.convertJi(self, res['ji']) in self.jiScope(ji)) and (element['qiFen'] == res['qiFen'] or element['qiFen'] == res['qiFenHuo']):
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
            ji = element['ji']
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                     'gongLiNian' : element['gongLiNian'],
                                        'name'    : element['name'],
                                        'keJu'    : element['keJu'],
                                        'minZu'   : element['minZu']})
        
            if res and (len(ji) > 0) and (Statistics.convertJi(self, res['ji']) in self.jiScope(ji)) and (element['qiFen'] == res['qiFen'] or element['qiFen'] == res['qiFenHuo']):
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
            ji = element['ji']
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                     'gongLiNian' : element['gongLiNian'],
                                        'name'    : element['name'],
                                        'keJu'    : element['keJu']})
        
            if res and (len(ji) > 0) and (Statistics.convertJi(self, res['ji']) in self.jiScope(ji)):
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
    threeJi = ThreeJi('table1', 'table2')
    threeJi.run()