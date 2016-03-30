# -*- coding: utf-8 -*-
from __future__ import division 
import logging
import pymongo
import os
from pypinyin import lazy_pinyin
import lib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


dataBase1 = r"C:\Users\elqstux\Desktop\study\History\database1.csv"
dataBase2 = r"C:\Users\elqstux\Desktop\study\History\database2.csv"
dataBase3 = r"C:\Users\elqstux\Desktop\study\History\database3.csv"

dbClient = pymongo.MongoClient()
db = dbClient.history
table1 = db.table1
table2 = db.table2
table3 = db.table3

field1 = ['jiJie', 'chaoDai', 'nian', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'yuanXingMing', 'minZu', 'qiFen', 'jueWei', 'keJu', 'name']
field2 = ['chaoDai', 'nian', 'yue', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'youMing', 'minZu', 'qiFen', 'qiFenHuo', 'jueWei', 'keJu', 'keJuHuo', 'name']
field3 = ['keNianGongLi', 'xingMing', 'qiFen', 'minZu', 'name']

def writeDB3(cvsPath):
    table3.remove()
    i = 0
    for line in open(cvsPath):
        if i == 0:
            i = i + 1
            continue
        values = line.replace('\n', '').split(';')
        
        keNianGongLi = values[2]
        xingMing     = values[8]
        qiFen        = values[12]
        minZu        = values[13]
        name         = '_'.join(lazy_pinyin(values[8].decode('utf-8')))
        
        element = zip(field3, [keNianGongLi, xingMing, qiFen, minZu, name])
        table3.insert(dict(element))
        i += 1
    logging.info(r"write db %s finished, lines = %d" % (cvsPath, i))
        
def writeCvsToDB(cvsPath, dbTable, field):
    dbTable.remove()
    i = 0
    result = []
    for line in open(cvsPath):
        if i == 0:   #skip the fisrt line for title 
            i = i + 1
            continue
        values = line.replace('\n', '').replace(' ', '').split(';')
        name = '_'.join(lazy_pinyin(values[11].decode('utf-8')))
        element = dict(zip(field, values + [name]))
        
        dbTable.insert(element)
        i += 1
    logging.info(r"write db %s finished, lines = %d" % (cvsPath, i))
    return result

if __name__ == '__main__':
    dirPath = os.path.join(os.path.dirname(__file__), u"步骤一重名者")
    if not os.path.exists(dirPath):
        os.mkdir(dirPath) 
    res1 = writeCvsToDB(dataBase1, table1, field1)
    lib.writeDataForTable1(open(os.path.join(dirPath, u"数据库1.txt"), "w"), res1)
    
    res2 = writeCvsToDB(dataBase2, table2, field2)
    lib.writeDataForTable2(open(os.path.join(dirPath, u"数据库2.txt"), "w"), res2)
    
    with open(os.path.join(dirPath, 'result.txt'), 'w') as file_:
        file_.write("数据库1: 重名人数: {}  总人数: {} 比例: {}\n".format(len(res1), lib.table1.count(), len(res1)/lib.table1.count()))
        file_.write("数据库2: 重名人数: {} 总人数: {} 比例: {}\n".format(len(res2), lib.table2.count(), len(res2)/lib.table2.count()))
    
    writeDB3(dataBase3)