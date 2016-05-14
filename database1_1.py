# -*- coding: utf-8 -*-
from __future__ import division
from statistics import Statistics 
import logging
import os


class Statistics1_1(Statistics):
    def __init__(self, dbName1, dbName2, year, rootDir = None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        self.total = self.table1.find({'ji': {'$ne': ""}}).count()
        
        rootDir = os.path.join(os.path.dirname(__file__), u"数据库1_1")

        if not os.path.exists(rootDir):
            os.mkdir(rootDir)    
        if year < 0:
            rootDir = os.path.join(rootDir, "提前{}year".format(abs(year)))
        elif year > 0:
            rootDir = os.path.join(rootDir, "延后{}year".format(abs(year)))
        else:
            rootDir = os.path.join(rootDir, "同一年year")
        if not os.path.exists(rootDir):
            os.mkdir(rootDir)    
        self.rootDir = rootDir
        
        self.year = year
            
        self.logger = logging.getLogger('Statistics1_1')
        self.logger.info("totol record: {}".format(self.total))
        
    def step1(self):
        resultDir = self.createDir(str(1))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            gongLiNian = int(element['gongLiNian'])
            gongLiNianScope = [str(gongLiNian + self.year)]
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
            gongLiNianScope = [str(gongLiNian + self.year)]
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
            gongLiNianScope = [str(gongLiNian + self.year)]
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
            gongLiNianScope = [str(gongLiNian + self.year)]
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
            gongLiNianScope = [str(gongLiNian + self.year)]
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
    
    def step6(self):
        if self.year != 0:
            return
        resultDir = self.createDir(str(6))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            res = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                        'name'    : element['name']})
            if res :
                match.write(self.formatElement(element))
                success += 1
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        print failed
        failed = self.total - success
        result = (success, success/self.total, failed, failed/self.total)    
        print '=========================='
        print result
        print '=========================='
        return result        

    def run(self):
        res1 = self.step1()
        res2 = self.step2()
        res3 = self.step3()
        res4 = self.step4()
        res5 = self.step5()
        self.step6()
        resultFile = open(os.path.join(self.rootDir, 'result.txt'), "w")
        index = 0
        titles = [ u"1:官职+姓名", u"2:官职+姓名+民族", u"3:官职+姓名+民族+旗分", u"4:官职+姓名+民族+旗分+科举", u"5:官职+姓名+科举"]
        for (a, b, c, d) in [res1, res2, res3, res4, res5]:
            resultFile.write("step{}: {} {} {} {}\n".format(titles[index], a, b, c, d))
            index += 1
if __name__ == '__main__':
    rootDir = os.path.join(__file__, u"数据库1_1")
    for i in [-3, -2, -1, 0, 1, 2, 3]:
        obj = Statistics1_1('table1_1', 'table2', i)
        obj.run()