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
        
        self.rootDir = os.path.join(self.rootDir, r"2.数据库2与数据库3对比")    
        if not os.path.exists(self.rootDir):
            os.mkdir(self.rootDir)
            
        self.logger = logging.getLogger('CompareDB2AndDB3')
        
        self.condition10to30 = self.extractData(-10, 30)
        self.condition20to40 = self.extractData(-20, 40)
        self.condition30to50 = self.extractData(-30, 50)
        
    def extractData(self, low, high):
        res1 = []
        res3 = []
        for x in self.table1.find():
            if len(x['gongLiNian']) == 0:
                continue
            year = int(x['gongLiNian'])
            y = self.table2.find({'xingMing': x['ming'],
                                  'keNianGongLi': {'$gte': str(year - high), '$lte': str(year - low)}
                                  })
            if y.count() > 0:
                res1.append(x)
                res3.extend(y)
        print "Done: {}__{}".format(low, high)
        return (res1, res3)
            
        
    def step1(self):
        resultDir = self.createDir(u"步骤1")
        match = open(os.path.join(resultDir, '数据库2有名字.txt'), "w")
        no_match = open(os.path.join(resultDir, '数据库2无名字.txt'), "w")
        match.write(lib.headers1 + '\n')
        no_match.write(lib.headers1 + '\n')
        
        match3 = []
        
        success = 0
        failed = 0    
        for element in self.table1.find({'keJu' : {'$in':["进士", "翻译进士"]}}):
            res = self.table2.find_one({'xingMing': element['ming']})
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
    
    def step2(self, low, high):
        dirName = "{}_{}".format(low, high)
        resultPath = os.path.join(self.rootDir, dirName)
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)
        resultDir = os.path.join(resultPath, u"步骤2")
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
            
        match = open(os.path.join(resultDir, '数据库2科举=进士或翻译进士.txt'), "w")
        no_match = open(os.path.join(resultDir, '数据库2科举≠进士或翻译进士.txt'), "w")
        match.write(lib.headers1 + '\n')
        no_match.write(lib.headers1 + '\n')
        
        if low == -10 and high == 30:
            res1, _res3 = self.condition10to30
        elif low == -20 and high == 40:
            res1, _res3 = self.condition20to40
        else:
            res1, _res3 = self.condition30to50
        
        diff1 = 0
        diff2 = 0
        for ele in res1:
            if ele['keJu'] in ["进士", "翻译进士"]:
                diff1 += 1
                match.write(self.formatElement2(ele))
            else:
                diff2 += 1
                no_match.write(self.formatElement2(ele))
        with open(os.path.join(resultDir, 'result.txt'), "w") as f_:
            f_.write("大前提:{}----{}\n".format(low, high))
            f_.write("1.科举=进士或翻译进士    {}/{}={}\n".format(diff1, len(res1), diff1 / len(res1)))
            f_.write("2.科举≠进士或翻译进士    {}/{}={}\n".format(diff2, len(res1), diff2 / len(res1)))
        print "Done: step2 {}_{}".format(low, high)
    
    def search(self, ele1, result3):
        result = []
        for ele in result3:
            if ele['xingMing'] == ele1['ming']:
                result.append(ele)
        return result
    def search_tab1(self, ele1, result1):
        result = []
        for ele in result1:
            if ele['ming'] == ele1['ming']:
                result.append(ele)
        return result
    
    def search0(self, ele1, result3):
        result = []
        for ele in result3:
            if ele['xingMing'] == ele1['ming'] and ele['minZu'] == ele1['minZu']:
                result.append(ele)
        return result
    def search1(self, ele1, result3):
        result = []
        for ele in result3:
            if ele['xingMing'] == ele1['ming'] and ele['qiFen'] == ele1['qiFen']:
                result.append(ele)
        return result
    def search2(self, ele1, result3):
        result = []
        for ele in result3:
            if ele['xingMing'] == ele1['ming'] and ele['qiFen'] == ele1['qiFen'] and ele['minZu'] == ele1['minZu']:
                result.append(ele)
        return result
    
    def step3(self, low, high):
        dirName = "{}_{}".format(low, high)
        resultPath = os.path.join(self.rootDir, dirName)
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)
        resultDir0 = os.path.join(resultPath, u"步骤3")
        if not os.path.exists(resultDir0):
            os.mkdir(resultDir0)
        file_ = open(os.path.join(resultDir0, "result.txt"), "w")
        
        if low == -10 and high == 30:
            res1, res3 = self.condition10to30
        elif low == -20 and high == 40:
            res1, res3 = self.condition20to40
        else:
            res1, res3 = self.condition30to50
            
        ###############################################
        resultDir = os.path.join(resultDir0, '姓名相同')
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        (match, no_match) = self.createResultFile(resultDir)
        
        match3 = open(os.path.join(resultDir, '数据库3match.txt'), 'w')
        match3.write(lib.headers3 + '\n')     
        
        success = 0
        failed = 0
        for ele1 in res1:
            res = self.search(ele1, res3)   
            if len(res) > 0:
                success += 1
                match.write(self.formatElement2(ele1))
                [match3.write(self.formatElement3(x)) for x in res] 
            else:
                no_match.write(self.formatElement2(ele1))
                failed += 1
        file_.write("姓名相同: {}\n".format(success))
        
        ###############################################
        resultDir = os.path.join(resultDir0, '姓名、民族相同')
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        (match, no_match) = self.createResultFile(resultDir)
        
        match3 = open(os.path.join(resultDir, '数据库3match.txt'), 'w')
        match3.write(lib.headers3 + '\n')     
        no_match3 = open(os.path.join(resultDir, '数据库3no_match.txt'), 'w')
        no_match3.write(lib.headers3 + '\n')
        
        success = 0
        failed = 0
        sameName3_0 = []
        
        for ele1 in res1:
            res = self.search0(ele1, res3)   
            if len(res) > 0:
                success += 1
                match.write(self.formatElement2(ele1))
                [match3.write(self.formatElement3(x)) for x in res]
                sameName3_0.extend(res) 
            else:
                no_match.write(self.formatElement2(ele1))
                failed += 1
        file_.write("姓名、民族相同: {}\n".format(success)) 
        [no_match3.write(self.formatElement3(x)) for x in res3 if x not in sameName3_0]
        
        ###############################################
        resultDir = os.path.join(resultDir0, '姓名、旗分相同')
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        (match, no_match) = self.createResultFile(resultDir)
        
        match3 = open(os.path.join(resultDir, '数据库3match.txt'), 'w')
        match3.write(lib.headers3 + '\n')     
        no_match3 = open(os.path.join(resultDir, '数据库3no_match.txt'), 'w')
        no_match3.write(lib.headers3 + '\n')
        
        success = 0
        failed = 0
        sameName3_1 = []
        
        for ele1 in res1:
            res = self.search1(ele1, res3)   
            if len(res) > 0:
                success += 1
                match.write(self.formatElement2(ele1))
                [match3.write(self.formatElement3(x)) for x in res]
                sameName3_1.extend(res) 
            else:
                no_match.write(self.formatElement2(ele1))
                failed += 1
        file_.write("姓名、旗分相同: {}\n".format(success)) 
        [no_match3.write(self.formatElement3(x)) for x in res3 if x not in sameName3_1]
        
        ###############################################
        resultDir = os.path.join(resultDir0, '姓名、民族、旗分相同')
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        (match, no_match) = self.createResultFile(resultDir)
        
        match3 = open(os.path.join(resultDir, '数据库3match.txt'), 'w')
        match3.write(lib.headers3 + '\n')     
        no_match3 = open(os.path.join(resultDir, '数据库3no_match.txt'), 'w')
        no_match3.write(lib.headers3 + '\n')
        
        success = 0
        failed = 0
        sameName3_2 = []
        
        for ele1 in res1:
            res = self.search2(ele1, res3)   
            if len(res) > 0:
                success += 1
                match.write(self.formatElement2(ele1))
                [match3.write(self.formatElement3(x)) for x in res]
                sameName3_2.extend(res) 
            else:
                no_match.write(self.formatElement2(ele1))
                failed += 1
        file_.write("姓名、民族、旗分相同: {}\n".format(success))
        [no_match3.write(self.formatElement3(x)) for x in res3 if x not in sameName3_1]
        
        print "Done: step3 {}_{}".format(low, high) 
            
    def step4(self, low, high):
        dirName = "{}_{}".format(low, high)
        resultPath = os.path.join(self.rootDir, dirName)
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)
        resultDir = os.path.join(resultPath, u"步骤4")
        if not os.path.exists(resultDir):
            os.mkdir(resultDir)
        
        file1 = open(os.path.join(resultDir, '数据库2时间差.txt'), 'w')
        file1.write("公历年时间差;" + lib.headers1 + "\n")
        
        file2 = open(os.path.join(resultDir, '数据库3时间差.txt'), 'w')
        file2.write("公历年时间差;" + lib.headers3 + "\n")
        
        if low == -10 and high == 30:
            res1, res3 = self.condition10to30
        elif low == -20 and high == 40:
            res1, res3 = self.condition20to40
        else:
            res1, res3 = self.condition30to50
        
        totalRes = []    
        for x in res1:
            if x['keJu'] == '进士' or x['keJu'] == '翻译进士':
                pass
            else:
                t1 = self.search_tab1(x, res1)
                t3 = self.search(x, res3)
                res = self.diffYear(list(t1), list(t3))
                totalRes.append(res)
                
        result2 = []        
        for (r1, r2, _) in totalRes:
            result2.extend(r1)
            line = []
            for key in ['diffYear'] + lib.keys3:
                line.append(r2[key])
            file2.write(';'.join(line) + '\n')
 
        for x in self.table1.find():
            z = self.checkName(x['ming'], result2)
            if not z:
                x['diffYear'] = ''
            else:
                x = z
            line = []
            for key in ['diffYear'] + lib.keys2:
                line.append(x[key])    
            file1.write(';'.join(line) + '\n')
    
        print "step4: {}_{} 数据时间差   total:{}".format(low, high, len(totalRes))  
    
    def diffYear(self, res1, res2):
        res2 = sorted(res2, key=lambda x: x["keNianGongLi"], reverse=True)
        maxElement = res2[0]
        maxYear = int(maxElement["keNianGongLi"])
        result = []
        for x in res1:
            if x['gongLiNian'] == '':
                x['diffYear'] = str(maxYear)
            else:
                x['diffYear'] = str(int(x['gongLiNian']) - maxYear)
            result.append(x)
            maxElement['diffYear'] = str(x['diffYear'])
        return (result, maxElement, maxYear)
    
    def checkName(self, name, elements):
        for y in elements:
            if y['ming'] == name:
                return y
        return None
                                    
    def run(self):
        self.step1()
        for (low, high) in [(-10, 30), (-20, 40), (-30, 50)]:
            self.step2(low, high)
            self.step3(low, high)
            self.step4(low, high)

if __name__ == '__main__':
    compareDb2AndDb3 = CompareDB1AndDB3('table2', 'table3')
    compareDb2AndDb3.run()
