# -*- coding: utf-8 -*-
import pymongo
headers1  = r"季节;朝代;年;公历年;季;衙门;官职;官职简写;官员;品级;身份;名;原姓名;民族;旗分;爵位;科举"
keys1     = ['jiJie', 'chaoDai', 'nian', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'yuanXingMing', 'minZu', 'qiFen', 'jueWei', 'keJu']
headers2  = r"朝代;年;月;公历年;季;衙门;官职;官职简写;官员;品级;身份;名;又名;民族;旗分;旗分（或）;爵位;科举;科举（其他）"
keys2     = ['chaoDai', 'nian', 'yue', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'youMing', 'minZu', 'qiFen', 'qiFenHuo', 'jueWei', 'keJu', 'keJuHuo']

headers3 = r"姓名;科年公歷;旗分;民族"
keys3    = ['xingMing', 'keNianGongLi', 'qiFen', 'minZu']

dbClient = pymongo.MongoClient()
db = dbClient.history
table1 = db.table1
table2 = db.table2
table3 = db.table3


RESULT_DIR = "C:\\Users\\elqstux\\Desktop\\study\\History\\统计结果 "

def writeDataForTable1(file_, data):
    file_.write(headers1 + '\n')
    for element in data:
        line = []
        for key in keys1:
            line.append(element[key])
        file_.write(';'.join(line) + '\n')
    file_.close()
    
def writeDataForTable2(file_, data):
    file_.write(headers2 + '\n')
    for element in data:
        line = []
        for key in keys2:
            line.append(element[key])
        file_.write(';'.join(line) + '\n')
    file_.close()
    
def writeDataForTable3(file_, data):
    file_.write(headers3 + '\n')
    for element in data:
        line = []
        for key in keys3:
            line.append(element[key])
        file_.write(';'.join(line) + '\n')
    file_.close()