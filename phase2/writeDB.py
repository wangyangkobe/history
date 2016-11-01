# -*- coding: utf-8 -*-
import logging
import pymongo
import os
import time
import lib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

csvPath = os.path.join(os.path.dirname(__file__), 'data.csv')
yearCsvPath = os.path.join(os.path.dirname(__file__), u'清朝纪元表.csv')

logger.info("csv file path: {}".format(csvPath))

dbClient = pymongo.MongoClient()


def makeYearMap(csvPath):
    yearMap = dict()
    lineNumber = 0
    with open(csvPath) as read:
        for line in read:
            lineNumber += 1
            fields = line.replace("\n", "").split(";")
            key = "{}-{}".format(fields[0], fields[1])
            yearMap[key] = fields[2]
    logger.info("lineNumber = {}, yearMap size = {}".format(
        lineNumber, len(yearMap)))
    return yearMap


def executeTime(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        logger.info("The time of executing function \'{}\' is {} 秒!".format(
            func.__name__, time.time() - start))
        return res
    return wrapper

def kakaMap(key):
    kaka = {"一": 1,
        "八": 8,
        "四十五": 45,
        "五十九": 59,
        "十三": 13,
        "十一": 11,
        "二十一": 21,
        "三十": 30,
        "十": 10,
        "三十三": 33,
        "三": 3,
        "三十七": 37,
        "五十三": 53,
        "四": 4,
        "九": 9,
        "二十三": 23,
        "二十二": 22,
        "四十四": 44,
        "四十九": 49,
        "五十": 50,
        "五十二": 52,
        "五十五": 55,
        "六十": 60,
        "七": 7,
        "十七": 17,
        "二": 2,
        "六": 6,
        "二十四": 24,
        "二十": 20,
        "八":8,
        "十四": 14,
        "十九":19,
        "二十一":21,
        "三十四":34}
    return kaka.get(key)

def handleAE(yearMap, renZhiChaoDai, renZhiNian, renZhiXiLiNian):
    if (not renZhiXiLiNian) and renZhiChaoDai and renZhiNian:
        key = "{}-{}".format(renZhiChaoDai, kakaMap(renZhiNian))
        #logger.info("running handleAE: renZhiNian = {}, key= {}, res = {}".format(renZhiNian, key, yearMap.get(key, "")))
        return yearMap.get(key, "")
    return renZhiXiLiNian

@executeTime
def csv2db(csvPath):
    dbClient.history.data.remove()
    isHeader = True
    headers = []
    elements = []
    yearMap = makeYearMap(yearCsvPath)
    with open(csvPath) as read:
        for line in read:
            line = line.replace("\n", "").replace(" ", "")
            if isHeader:
                isHeader = False
                headers = line.split(';')
                logger.info(line)
            else:
                values = line.split(';')
                element = dict(zip(headers, values))

                key1 = "{}-{}".format(element["时间1朝代"], element["时间1年"])
                element['时间1公历年'] = yearMap.get(key1, "")
                key2 = "{}-{}".format(element["时间2朝代"], element["时间2年"])
                element['时间2公历年'] = yearMap.get(key2, "")

                element['任职西历年'] = handleAE(yearMap, element["任职朝代"], element["任职年"], element["任职西历年"])

                elements.append(element)
    dbClient.history.data.insert(elements)
    logger.info('finish csv2db, element number is {}'.format(
        dbClient.history.data.count()))

if __name__ == '__main__':
    csv2db(csvPath)

