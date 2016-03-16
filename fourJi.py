# -*- coding: utf-8 -*-
from __future__ import division
from statistics import Statistics 
import logging
import os

class FourJi(Statistics):
    def __init__(self, dbName1, dbName2, rootDir = None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        self.total = self.table1.find({'ji': {'$ne': ""}}).count()
        self.rootDir = os.path.join(self.rootDir, u"ͬ间隔一年")
        self.logger = logging.getLogger('FourJi')
        
    def step1(self):
        resultDir = self.createDir(str(1))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            ji = element['ji']
            if self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'ji'      : {'$in' : [ji, "", str(int(ji)-1), str(int(ji)+1), str(int(ji)-2), str(int(ji)+2), str(int(ji)-3), str(int(ji)+3)]},
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
        resultDir = self.createDir(str(2))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            ji = element['ji']
            if self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'name'    : element['name'],
                                     'ji'      : {'$in' : [ji, "", str(int(ji)-1), str(int(ji)+1), str(int(ji)-2), str(int(ji)+2), str(int(ji)-3), str(int(ji)+3)]},
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
        resultDir = self.createDir(str(3))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            ji = element['ji']
            result = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                           'name'    : element['name'],
                                           'ji'      : {'$in' : [ji, "", str(int(ji)-1), str(int(ji)+1), str(int(ji)-2), str(int(ji)+2), str(int(ji)-3), str(int(ji)+3)]},
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
        resultDir = self.createDir(str(4))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            ji = element['ji']
            result = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                           'name'    : element['name'],
                                           'ji'      : {'$in' : [ji, "", str(int(ji)-1), str(int(ji)+1), str(int(ji)-2), str(int(ji)+2), str(int(ji)-3), str(int(ji)+3)]},
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
        resultDir = self.createDir(str(5))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': {'$ne': ""}}):
            ji = element['ji']
            result = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                           'name'    : element['name'],
                                           'ji'      : {'$in' : [ji, "", str(int(ji)-1), str(int(ji)+1), str(int(ji)-2), str(int(ji)+2), str(int(ji)-3), str(int(ji)+3)]},
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
        index = 1
        for (a, b, c, d) in [res1, res2, res3, res4, res5]:
            resultFile.write("step{}: {} {} {} {}\n".format(index, a, b, c, d))
            index += 1

if __name__ == '__main__':
    fourJi = FourJi('table1', 'table2')
    fourJi.run()