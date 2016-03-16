# -*- coding: utf-8 -*-
from __future__ import division 
import logging
import os
from statistics import Statistics

class CompareDB1AndDB3(Statistics):
    def __init__(self, dbName1, dbName2, rootDir = None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        self.total = self.table1.count()
        self.rootDir = os.path.join(self.rootDir, u"数据库1和数据库3")
        self.logger = logging.getLogger('CompareDB1AndDB3')
    
    def step1(self):
        resultDir = self.createDir(str(1))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find():
            if element['keJu'] == "":
                no_match.write(self.formatElement(element))
                failed += 1 
            else:
                result = self.table2.find({'xingMingPinYin': element['name']})
                
                if result:
                    isMatched = False
                    for item in result:
                        if item['ruShiLeiBie'].find(element['keJu']) != -1:
                            match.write(self.formatElement(element))
                            success += 1
                            isMatched = True
                            break
                    if not isMatched:
                        no_match.write(self.formatElement(element))
                        failed += 1
                else:
                    no_match.write(self.formatElement(element))
                    failed += 1       
    
        result = (success, success/self.total, failed, failed/self.total)
        self.logResult(1, result) 
        return result
    
    def run(self):
        res1 = self.step1()
        resultFile = open(os.path.join(self.rootDir, 'result.txt'), "w")
        index = 1
        for (a, b, c, d) in [res1]:
            resultFile.write("step{}: {} {} {} {}\n".format(index, a, b, c, d))
            index += 1
            
class CompareDB2AndDB3(Statistics):
    def __init__(self, dbName1, dbName2, rootDir = None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        self.total = self.table1.count()
        self.rootDir = os.path.join(self.rootDir, u"数据库2和数据库3")
        self.logger = logging.getLogger('CompareDB2AndDB3')
    
    def createResultFile(self, dirName): 
        self.logger.info("{} {}".format("createResultFile", dirName))
        matched    = open(os.path.join(dirName, 'match.txt'),    "w")
        notMatched = open(os.path.join(dirName, 'no_match.txt'), "w")
    
        matched.write(Statistics.headers2 + '\n')
        notMatched.write(Statistics.headers2 + '\n')
        return (matched, notMatched)
    
    def step1(self):
        resultDir = self.createDir(str(1))
        (match, no_match) = self.createResultFile(resultDir)
    
        success = 0
        failed  = 0    
        for element in self.table1.find():
            if element['keJu'] == "":
                no_match.write(self.formatElement2(element))
                failed += 1 
            else:
                result = self.table2.find({'xingMingPinYin': element['name']})
                
                if result:
                    isMatched = False
                    for item in result:
                        if item['ruShiLeiBie'].find(element['keJu']) != -1:
                            match.write(self.formatElement2(element))
                            success += 1
                            isMatched = True
                            break
                    if not isMatched:
                        no_match.write(self.formatElement2(element))
                        failed += 1
                else:
                    no_match.write(self.formatElement2(element))
                    failed += 1  
        result = (success, success/self.total, failed, failed/self.total)
        self.logResult(1, result) 
        return result
    
    def run(self):
        res1 = self.step1()
        resultFile = open(os.path.join(self.rootDir, 'result.txt'), "w")
        index = 1
        for (a, b, c, d) in [res1]:
            resultFile.write("step{}: {} {} {} {}\n".format(index, a, b, c, d))
            index += 1
                        
if __name__ == "__main__":
    compareDb1AndDb3 = CompareDB1AndDB3('table1', 'table3')
    compareDb1AndDb3.run()
    compareDb2AndDb3 = CompareDB2AndDB3('table2', 'table3')
    compareDb2AndDb3.run()