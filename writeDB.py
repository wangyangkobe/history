# -*- coding: utf-8 -*-
import logging
import pymongo
from pypinyin import lazy_pinyin

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

field1 = ['jiJie', 'chaoDai', 'nian', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'minZu', 'qiFen', 'jueWei', 'keJu', 'name']
field2 = ['chaoDai', 'nian', 'yue', 'gongLiNian', 'ji', 'yaMen', 'guanZhi', 'guanZhiJianXie', 'guanYuan', 'pinJi', 'shengFen', 'ming', 'youMing', 'minZu', 'qiFen', 'qiFenHuo', 'jueWei', 'keJu', 'keJuHuo', 'name']
field3 = ['ruShiLeiBie', 'xingMing', 'name', 'xingMingPinYin']
#with open(dataBase1, 'rb') as csvFile:
#    spamreader = csv.reader(csvFile, delimiter=',', quotechar='|')
#    for row in spamreader:
#        print row


def writeDB3(cvsPath):
    table3.remove()
    i = 0
    for line in open(cvsPath):
        if i == 0:
            i = i + 1
            continue
        values = line.replace('\n', '').split(';')
        
        ruShiLeiBie    = values[2]
        xingMingPinYin = values[29]
        xingMing       = values[30]
        name           = '_'.join(lazy_pinyin(values[30].decode('utf-8')))
        
        element = zip(field3, [ruShiLeiBie, xingMingPinYin, xingMing, name])
        table3.insert(dict(element))
        i += 1
    logging.info(r"write db %s finished, lines = %d" % (cvsPath, i))
        
def writeCvsToDB(cvsPath, dbTable, field):
    dbTable.remove()
    i = 0
    for line in open(cvsPath):
        if i == 0:
            #skip the fisrt line for title
            i = i + 1
            continue
        values = line.replace('\n', '').replace(' ', '').split(';')
        
        name = '_'.join(lazy_pinyin(values[11].decode('utf-8')))
        element = zip(field, values + [name])
        
        dbTable.insert(dict(element))
        i += 1
    logging.info(r"write db %s finished, lines = %d" % (cvsPath, i))
    

if __name__ == '__main__':
    writeCvsToDB(dataBase1, table1, field1)
    writeCvsToDB(dataBase2, table2, field2)
    writeDB3(dataBase3)