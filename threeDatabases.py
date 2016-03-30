# -*- coding: utf-8 -*-
from __future__ import division 
import logging
import os
from statistics import Statistics
import lib

class CompareDB1AndDB3(Statistics):
    def __init__(self, dbName1, dbName2, rootDir=None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        self.total = self.table1.count()
        
        self.rootDir = os.path.join(self.rootDir, u"步骤三数据库数据的匹配程度")
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, r"3.数据库1、2与数据库3的比对")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, r"1.数据库1与数据库3对比")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
            
        self.logger = logging.getLogger('CompareDB1AndDB3')
    
    def step1(self):
        resultDir = self.createDir(u"步骤1")
        # (match, no_match) = self.createResultFile(resultDir)
        match = open(os.path.join(resultDir, '数据库1有名字.txt'), "w")
        no_match = open(os.path.join(resultDir, '数据库1无名字.txt'), "w")
        match.write(lib.headers1 + '\n')
        no_match.write(lib.headers1 + '\n')
        
        match3 = []
        
        success = 0
        failed = 0    
        for element in self.table1.find({'keJu' : {'$in':["进士", "翻译进士"]}}):
            res = self.table2.find_one({'name': element['name']})
            if res:
                success += 1
                match.write(self.formatElement(element))
                match3.append(res)
            else:
                failed += 1
                no_match.write(self.formatElement(element))
        total = success + failed
        result = (success, success / total, failed, failed / total)
        
        with open(os.path.join(resultDir, '数据库3match.txt'), "w") as f_:
            lib.writeDataForTable3(f_, match3)
        with open(os.path.join(resultDir, 'result.txt'), "w") as f_:
            f_.write("前提: {} 总数: {} 百分比: {}\n".format(success + failed, self.table1.count(), (success + failed) / self.table1.count()))
            f_.write("match: {} not_match: {} rate: {}\n".format(success, failed, success / total))
            
        print "step1 -- total: {}, match: {}, no_matched: {}, rate: {}".format(total, success, failed, success / total)
        return result
    
    def step2(self):
        resultDir = self.createDir(u"步骤2")
        sameNames1 = []
        sameNames3 = []
        
        match = open(os.path.join(resultDir, '数据库1科举=进士或翻译进士.txt'), "w")
        no_match = open(os.path.join(resultDir, '数据库1科举≠进士或翻译进士.txt'), "w")
        match.write(lib.headers1 + '\n')
        no_match.write(lib.headers1 + '\n')
        
        for r1 in self.table1.find():
            r2 = self.table2.find_one({'name': r1['name']})
            if r2:
                sameNames1.append(r1)
                sameNames3.append(r2)
        diff1 = 0
        diff2 = 0
        for ele in sameNames1:
            if ele['keJu'] in ["进士", "翻译进士"]:
                diff1 += 1
                match.write(self.formatElement(ele))
            else:
                diff2 += 1
                no_match.write(self.formatElement(ele))
        
        with open(os.path.join(resultDir, 'result.txt'), "w") as f_:
            f_.write("前提：数据库1和3有共同的\"姓名\"  {}/{}={}\n".format(len(sameNames1), self.table1.count(), len(sameNames1) / self.table1.count()))
            f_.write("1.科举=进士或翻译进士    {}/{}={}\n".format(diff1, len(sameNames1), diff1 / len(sameNames1)))
            f_.write("2.科举≠进士或翻译进士    {}/{}={}\n".format(diff2, len(sameNames1), diff2 / len(sameNames1)))
            
        print "step2 -- total: {}, match: {}, no_matched: {}, rate: {}".format(len(sameNames1), diff1, diff2, diff1 / len(sameNames1))       
    
    def diffYear(self, results, year):
        maxYear = 0
        result = {}
        for ele in results:
            if int(ele['gongLiNian']) > maxYear:
                maxYear = int(ele['gongLiNian'])
                result.update(ele)
        result['diffYear'] = str(int(year) - maxYear)
        return result
        
    def step4(self):
        resultDir = self.createDir(str("步骤4"))
        file_ = open(os.path.join(resultDir, '时间差.txt'), 'w')
        file_.write("姓名;数据库1时间;数据库3时间;时间差\n")
        total = 0  
        for element in self.table1.find({'keJu' : {'$nin':["进士", "翻译进士"]}}):
            res = self.table2.find_one({'name': element['name']})
            if res:
                total += 1
                res1 = self.table1.find({'name': element['name'], 'keJu' : {'$nin':["进士", "翻译进士"]}})
                diffYear = self.diffYear(res1, res['keNianGongLi'])
                file_.write("{};{};{};{}\n".format(element['ming'], element['gongLiNian'], res['keNianGongLi'], diffYear['diffYear']))
        print "step4: 数据时间差   total:{}".format(total)
            
    def step5(self):
        resultDir0 = self.createDir(str("步骤3"))
        file_ = open(os.path.join(resultDir0, "result.txt"), "w")
        
        ###############################################
        resultDir = os.path.join(resultDir0, '姓名相同')
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        (match, no_match) = self.createResultFile(resultDir)
        
        match3 = open(os.path.join(resultDir, '数据库3match.txt'), 'w')
        match3.write(lib.headers3 + '\n')
        
        success = 0
        failed = 0
        for element in self.table1.find():
            r3 = self.table2.find_one({'name': element['name']})
            if r3:
                success += 1
                match.write(self.formatElement(element))
                match3.write(self.formatElement3(r3))
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        file_.write("姓名相同: {}\n".format(success))     
        
        ###############################################
        resultDir = os.path.join(resultDir0, '姓名、民族相同')
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        (match, no_match) = self.createResultFile(resultDir)
        
        match3 = open(os.path.join(resultDir, '数据库3match.txt'), 'w')
        match3.write(lib.headers3 + '\n')
        
        success = 0
        failed = 0
        for element in self.table1.find():
            r3 = self.table2.find_one({'name': element['name'], 'minZu': element['minZu']})
            if r3:
                success += 1
                match.write(self.formatElement(element))
                match3.write(self.formatElement3(r3))
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        file_.write("姓名、民族相同: {}\n".format(success)) 
        
        ###############################################
        resultDir = os.path.join(resultDir0, '姓名、旗分相同')
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        (match, no_match) = self.createResultFile(resultDir)
        match3 = open(os.path.join(resultDir, '数据库3match.txt'), 'w')
        match3.write(lib.headers3 + '\n')
        
        success = 0
        failed = 0
        for element in self.table1.find():
            r3 = self.table2.find_one({'name': element['name'], 'qiFen': element['qiFen']})
            if r3:
                success += 1
                match.write(self.formatElement(element))
                match3.write(self.formatElement3(r3))
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        file_.write("姓名、旗分相同: {}\n".format(success)) 
        
        ###############################################
        resultDir = os.path.join(resultDir0, '姓名、民族、旗分相同')
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        (match, no_match) = self.createResultFile(resultDir)
        match3 = open(os.path.join(resultDir, '数据库3match.txt'), 'w')
        match3.write(lib.headers3 + '\n')
        
        success = 0
        failed = 0
        for element in self.table1.find():
            r3 = self.table2.find_one({'name': element['name'], 'minZu': element['minZu'], 'qiFen': element['qiFen']})
            if r3:
                success += 1
                match.write(self.formatElement(element))
                match3.write(self.formatElement3(r3))
            else:
                no_match.write(self.formatElement(element))
                failed += 1
        file_.write("姓名、民族、旗分相同: {}\n".format(success))    
        
        
        print "Done!"     
        
                        
    def run(self):
        self.step1()
        self.step2()
        self.step4()
        self.step5()
        #=======================================================================
        # resultFile = open(os.path.join(self.rootDir, 'result.txt'), "w")
        # index = 1
        # for (a, b, c, d) in [res1]:
        #     resultFile.write("step{}: {} {} {} {}\n".format(index, a, b, c, d))
        #     index += 1
        #=======================================================================
            
class CompareDB2AndDB3(Statistics):
    def __init__(self, dbName1, dbName2, rootDir=None):
        Statistics.__init__(self, dbName1, dbName2, rootDir)
        self.total = self.table1.count()
        self.rootDir = os.path.join(self.rootDir, u"步骤三数据库数据的匹配程度")
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, r"3.数据库1、2与数据库3的比对")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
        
        self.rootDir = os.path.join(self.rootDir, r"2.数据库2与数据库3对比")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
            
        self.logger = logging.getLogger('CompareDB2AndDB3')
    
    def step1(self):
        resultDir = self.createDir(u"步骤1")
        # (match, no_match) = self.createResultFile(resultDir)
        match = open(os.path.join(resultDir, '数据库2有名字.txt'), "w")
        no_match = open(os.path.join(resultDir, '数据库2无名字.txt'), "w")
        match.write(lib.headers1 + '\n')
        no_match.write(lib.headers1 + '\n')
        
        match3 = []
        
        success = 0
        failed = 0    
        for element in self.table1.find({'keJu' : {'$in':["进士", "翻译进士"]}}):
            res = self.table2.find_one({'name': element['name']})
            if res:
                success += 1
                match.write(self.formatElement2(element))
                match3.append(res)
            else:
                failed += 1
                no_match.write(self.formatElement2(element))
        total = success + failed
        result = (success, success / total, failed, failed / total)
        
        with open(os.path.join(resultDir, '数据库3match.txt'), "w") as f_:
            lib.writeDataForTable3(f_, match3)
        with open(os.path.join(resultDir, 'result.txt'), "w") as f_:
            f_.write("前提: {} 总数: {} 百分比: {}\n".format(success + failed, self.table1.count(), (success + failed) / self.table1.count()))
            f_.write("match: {} not_match: {} rate: {}\n".format(success, failed, success / total))
            
        print "step1 -- total: {}, match: {}, no_matched: {}, rate: {}".format(total, success, failed, success / total)
        return result
    

    def step2(self):
        resultDir = self.createDir(u"步骤2")
        sameNames1 = []
        sameNames3 = []
        
        match = open(os.path.join(resultDir, '数据库2科举=进士或翻译进士.txt'), "w")
        no_match = open(os.path.join(resultDir, '数据库2科举≠进士或翻译进士.txt'), "w")
        match.write(lib.headers1 + '\n')
        no_match.write(lib.headers1 + '\n')
        
        for r1 in self.table1.find():
            r2 = self.table2.find_one({'name': r1['name']})
            if r2:
                sameNames1.append(r1)
                sameNames3.append(r2)
        diff1 = 0
        diff2 = 0
        for ele in sameNames1:
            if ele['keJu'] in ["进士", "翻译进士"]:
                diff1 += 1
                match.write(self.formatElement2(ele))
            else:
                diff2 += 1
                no_match.write(self.formatElement2(ele))
        
        with open(os.path.join(resultDir, 'result.txt'), "w") as f_:
            f_.write("前提：数据库2和3有共同的\"姓名\"  {}/{}={}\n".format(len(sameNames1), self.table1.count(), len(sameNames1) / self.table1.count()))
            f_.write("1.科举=进士或翻译进士    {}/{}={}\n".format(diff1, len(sameNames1), diff1 / len(sameNames1)))
            f_.write("2.科举≠进士或翻译进士    {}/{}={}\n".format(diff2, len(sameNames1), diff2 / len(sameNames1)))
            
        print "step2 -- total: {}, match: {}, no_matched: {}, rate: {}".format(len(sameNames1), diff1, diff2, diff1 / len(sameNames1))  
 
    def step4(self):
        resultDir = self.createDir(str("步骤4"))
        file_ = open(os.path.join(resultDir, '时间差.txt'), 'w')
        file_.write("姓名;数据库2时间;数据库3时间;时间差\n")
        total = 0  
        for element in self.table1.find({'keJu' : {'$nin':["进士", "翻译进士"]}, 'gongLiNian': {'$ne': ""}}):
            res = self.table2.find_one({'name': element['name']})
            if res:
                total += 1
                res1 = self.table1.find({'name': element['name'], 'gongLiNian': {'$ne': ""}, 'keJu' : {'$nin':["进士", "翻译进士"]}})
                diffYear = self.diffYear(res1, res['keNianGongLi'])
                file_.write("{};{};{};{}\n".format(element['ming'], element['gongLiNian'], res['keNianGongLi'], diffYear['diffYear']))
        print "step4: 数据时间差   total:{}".format(total)
    
    def diffYear(self, results, year):
        maxYear = 0
        result = {}
        for ele in results:
            if int(ele['gongLiNian']) > maxYear:
                maxYear = int(ele['gongLiNian'])
                result.update(ele)
        result['diffYear'] = str(int(year) - maxYear)
        return result
    
    def run(self):
        self.step1()
        self.step2()
        self.step4()
if __name__ == "__main__":
    compareDb1AndDb3 = CompareDB1AndDB3('table1', 'table3')
    compareDb1AndDb3.run()
    compareDb2AndDb3 = CompareDB2AndDB3('table2', 'table3')
    compareDb2AndDb3.run()
