# -*- coding: utf-8 -*-
from __future__ import division 
from statistics import Statistics
import os
import logging
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

class JiEqualNull(Statistics):
    def __init__(self, dbName1, dbName2, rootDir = None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        self.total = self.table1.count()
        self.rootDir = os.path.join(self.rootDir, u"步骤三数据库数据的匹配程度")
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"2.不考虑公历年")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"2.考虑季节")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, u"1.季为空")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)    
        self.logger = logging.getLogger('JiEqualNull')
    
    def step0(self):
        i = 0
        j = 0
        (match, no_match) = self.createResultFile(self.rootDir)
        for element in self.table2.find():
            if len(element['ji']) == 0:
                i += 1
                match.write(self.formatElement2(element))
            else:
                j += 1
                no_match.write(self.formatElement2(element))
        resultFile = open(os.path.join(self.rootDir, 'result.txt'), "w")
        resultFile.write("数据库2季为空的数据有: {}, 共  {}, 比例为: {}".format(i, i+j, i/(i+j)))
        
    def step1(self):
        resultDir = self.createDir(str(1))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find({'ji': ""}):
            if self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'name'    : element['name'],
                                     'nian'    : element['nian']}):
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
        for element in self.table1.find({'ji': ""}):
            if self.table2.find_one({'guanZhi' : element['guanZhi'],
                                     'name'    : element['name'],
                                     'minZu'   : element['minZu'],
                                     'nian'    : element['nian']}):
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
        for element in self.table1.find({'ji': ""}):
            result = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                           'name'    : element['name'],
                                           'nian'    : element['nian'],
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
        for element in self.table1.find({'ji': ""}):
            result = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                           'name'    : element['name'],
                                           'nian'    : element['nian'],
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
        for element in self.table1.find({'ji': ""}):
            result = self.table2.find_one({'guanZhi' : element['guanZhi'], 
                                           'name'    : element['name'],
                                           'nian'    : element['nian'],
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
        #=======================================================================
        # res1 = self.step1()
        # res2 = self.step2()
        # res3 = self.step3()
        # res4 = self.step4()
        # res5 = self.step5()
        # resultFile = open(os.path.join(self.rootDir, 'result.txt'), "w")
        # index = 1
        # for (a, b, c, d) in [res1, res2, res3, res4, res5]:
        #     resultFile.write("step{}: {} {} {} {}\n".format(index, a, b, c, d))
        #     index += 1
        #=======================================================================
        self.step0()

if __name__ == '__main__':
    jiEqualNull = JiEqualNull('table1', 'table2')
    jiEqualNull.run()
    
