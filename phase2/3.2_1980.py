# -*- coding: utf-8 -*-
from __future__ import division
import logging
import pymongo
import os
import time
import json
from itertools import *
from operator import *
import pandas as pd

import sys
try:
    reload(sys)
    sys.setdefaultencoding('utf-8')
except:
    pass
HEADER = "履历类别;履历编码;姓名内编号;时间1朝代;时间1年;时间1公历年;时间2朝代;时间2年;时间2公历年;任职方式;任职地;任职职位;任职职位简写;履历特点;职位总序号;官职类别-编码1;来源;页码;衙门;官职类别;官职类别（124）-编码1;地区;地区-编码2;官职名称;官职内-编码3;品级;姓名;又名;任职朝代;任职年;任职西历年;任职月;任职季;任职日;任职西历时间;任职前职位;任职类别;离职朝代;离职年;离职西历年;离职月;离职季;离职日;离职西历时间;离职原因;离职原因简写;备注;出处-person ID;出处-人名权威资料;出处其他;生年;卒年;族;字;号;谥号;曾祖;祖父;父;兄1;兄2;兄3;兄4;兄5;兄6;兄7;兄8;兄9;兄10;兄11;子1;子2;子3;子4;子5;子6;子7;子8;子9;子10;子11;子12;子13;子14;子15;子16;孙1;孙2;孙3;孙4;孙5;孙6;孙7;孙8;孙9;孙10;孙11;孙12;孙13;孙14;孙15;孙16;民族;旗分;先赋;科举朝代;科年;科年公历;科举功名;世袭世职;其他出身;籍贯省;籍贯市;籍贯其他"
RESTULT_HEADER = "进士数量;进士比例;举人数量;举人比例;举人以下数量;举人以下比例;空白数量;空白比例"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dbClient = pymongo.MongoClient()
db = dbClient.history


def search(L, target):
    for element in L:
        value = element.get(target)
        if value:
            return value
    return None

def getFieldValues(fieldName):
    res = db['1980'].distinct(fieldName)
    #logger.info(u"running getFieldValues, fieldName = {}, values = {}".format(fieldName, json.dumps(res, ensure_ascii=False, indent=2)))
    return res

def replaceNullStr(str):
    if not str:
        return "空白"
    else:
        return str

def selectWorker(typeName, row):
    keJun = db['1980'].distinct(u"科举功名")

    jinShi = [u"进士", u"武进士", u"翻译进士"]
    juRen  = [u"举人", u"武举人", u"翻译举人"]
    belowJuRen = list(set(keJun) - set(jinShi) - set(juRen) - set([""]))

    #columnValues = [u"进士", u"举人", u"武进士", u"翻译进士", u"翻译举人", u"翻译生员", ""]
    assert len(jinShi) + len(juRen) + len(belowJuRen) + 1 == len(keJun)

    logger.info(u"running selectWorker, typeName = {}, row = {}".format(typeName, replaceNullStr(row)))

    jinShi         = list(db['1980'].find({typeName: row, "科举功名": {'$in': jinShi}}))
    juRen          = list(db['1980'].find({typeName: row, "科举功名": {'$in': juRen}}))
    belowJuRen     = list(db['1980'].find({typeName: row, "科举功名": {'$in': belowJuRen}}))
    kongBai        = list(db['1980'].find({typeName: row, "科举功名": ""}))

    return zip(["进士", "举人", "举人以下", "空白"],
               [jinShi, juRen, belowJuRen, kongBai])


def prepareDir(dirName):
    dirPath = os.path.join(os.path.dirname(__file__), dirName)
    logger.info("\nrunning prepareDir, dirPath = {}".format(os.path.abspath(dirPath)))
    if not os.path.exists(dirPath):
        os.mkdir(dirPath)
    return dirPath

def writeFile(fcout, L):
    fcout.write(HEADER + "\n")
    headers = HEADER.split(";")
    for element in L:
        values = []
        for header in headers:
            values.append(element.get(u"{}".format(header), ""))
        fcout.write(";".join(values) + "\n")
    
def generateReport(typeName):
    rows = getFieldValues(typeName)
    baseDir = prepareDir(u"3_2_科举与职位(1980)")
    baseDir = os.path.join(baseDir, "{}".format(typeName))
    if not os.path.exists(baseDir):
        os.mkdir(baseDir)
    logger.info("running generateReport: tyepName={}, rows size={}, dirPath={}".format(
        typeName, len(rows), os.path.abspath(baseDir)))
    writer = pd.ExcelWriter(os.path.join(baseDir, 'result.xlsx'))

    total = db['1980'].count()
    res = []
    for row in rows:
        line = []
        line.append(replaceNullStr(row))
        for key, value in selectWorker(typeName, row):
            with open(os.path.join(baseDir, "{}_{}.txt".format(replaceNullStr(row), key)), "w") as fcout:
                writeFile(fcout, value)

            line.append(len(value))
            line.append(len(value) / total) 
        res.append(line)

    df = pd.DataFrame(res, columns=(typeName + ";" + RESTULT_HEADER).split(";"))
    df.to_excel(writer, index=False)
    writer.save()    



def run():
    for element in [u"官职名称", u"地区", u"官职类别", u"民族", u"品级"]:
        generateReport(element)

if __name__ == "__main__":
    run()
