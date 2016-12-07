# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import unicode_literals

import logging
import pymongo
import pandas as pd
from itertools import groupby, izip_longest
from collections import Counter, defaultdict
import os
import pprint
import sys

reload(sys)
sys.setdefaultencoding('utf8')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dbClient = pymongo.MongoClient()
db = dbClient.history

baseDir = os.path.join(os.path.dirname(__file__), "2_基本数据统计情况")
logger.info("baseDir = {}".format(os.path.abspath(baseDir)))
if not os.path.exists(baseDir):
	os.mkdir(baseDir)

def precondition():
    res = list(db.data.find({"姓名内编号": {'$in': ["0", "1"]}}))
    db['1980'].drop()
    db['1980'].insert(res)
    logger.info("running precondition, table '1980' size is {}.".format(len(res)))

def replaceNullString(L):
	res = []
	for x in L:
		if not x:
			res.append(u"空白")
		else:
			res.append(x)
	return res

def step1():
	headers = [u"官职名称", u"地区", u"官职类别", u"品级", u"科举功名"]
	gunanZhiName  = replaceNullString( list(db['1980'].distinct("官职名称")) )
	diQu          = replaceNullString( list(db['1980'].distinct("地区")) )
	guanZhiLieBie = replaceNullString( list(db['1980'].distinct("官职类别")) )
	piJi          = replaceNullString( list(db['1980'].distinct("品级")) )
	keJu          = replaceNullString( list(db['1980'].distinct("科举功名")) )

	numbers = [len(gunanZhiName), len(diQu), len(guanZhiLieBie), len(piJi), len(keJu)]
	logging.info("running step1: {}".format(numbers))

	df = pd.DataFrame( [numbers], columns=headers)
	table = list(izip_longest(gunanZhiName, diQu, guanZhiLieBie, piJi, keJu))
	df = df.append( pd.DataFrame(table, columns=headers) )
	writer = pd.ExcelWriter(os.path.join(baseDir, '1.数据类别的统计.xlsx'))
	df.to_excel(writer,'Sheet1',index=False)
	writer.save()

	for name, types in zip(headers, [gunanZhiName, diQu, guanZhiLieBie, piJi, keJu]):
		res = dict()
		for element in types:
			if element == u"空白":
				number = db['1980'].find({name : ""}).count()
			else:
				number = db['1980'].find({name : element}).count()
			res[element] = number

		df = pd.DataFrame(res.items(), columns=['类别', '数量'])
		df.to_excel(writer, name, index=False)
		writer.save()
		
def step2():
	logger.info("running step2!")
	writer = pd.ExcelWriter(os.path.join(baseDir, '2.官职姓名重复的情况.xlsx'))

	writer1 = pd.ExcelWriter(os.path.join(baseDir, '姓名.xlsx'))
	total = db['1980'].count()
	resMap = defaultdict(list)
	xingMings = [element['姓名'] for element in db['1980'].find()]
	counter = list(reversed(Counter(xingMings).most_common()))
	for times, group in groupby(counter, lambda x: x[1]):
		values = list(group)
		resMap[times].extend([times, len(values), len(values) / total])
		res = []
		for x in values:
			for y in db['1980'].find({'姓名': x[0]}):
				res.append((x[0], y['官职名称'], y['任职西历年']))
		df = pd.DataFrame(res, columns=['姓名','官职名称','任职西历年'])
		df.to_excel(writer1, "{}次".format(times), index=False)
		writer1.save()

	gzmcAndxm = [(element['官职名称'], element['姓名']) for element in db['1980'].find()]
	counter = list(reversed(Counter(gzmcAndxm).most_common()))

	for times, group in groupby(counter, lambda x: x[1]):
		values = list(group)
		if times in resMap:
			resMap[times].extend([len(values), len(values) / total])
		else:
			resMap[times].extend([times, None, None, len(values), len(values) / total])

		selectRes = list()
		for x, y in [element[0] for element in values]:
			selectRes.extend(list(db['1980'].find({'官职名称': x, '姓名': y})))
		selectRes = [(x['官职名称'], x['姓名'], int(x['任职西历年']) if x['任职西历年'] else None) for x in selectRes]
		df = pd.DataFrame(selectRes, columns=['官职名称', '姓名', '任职西历年'])
		df.to_excel(writer, "{}次".format(times), index=False)
		writer.save()

	third = list(set((element['姓名'], element['官职名称']) for element in db['1980'].find()))
	third = sorted(third, key = lambda x: x[0])
	tmpRes = []
	for name, group in groupby(third, lambda x: x[0]):
		values = list(group)
		tmpRes.append((name, len(values), values))
	tmpRes = sorted(tmpRes, key=lambda x: x[1])	
	for times, group in groupby(tmpRes, lambda x: x[1]):
		values = list(group)
		logger.info("{} times, number = {}.".format(times, len(values)))
		if times in resMap:
			if len(resMap[times]) < 4:
				resMap[times].extend([None, None])
			resMap[times].extend([len(values), len(values) / total])
		else:
			resMap[times].extend([times, None, None, None, None, len(values), len(values) / total])

		result = []	
		for (_, _, interValues) in values:
			for name, gunanZhiName in interValues:
				selectRes = db['1980'].find({'官职名称': gunanZhiName, '姓名': name})
				selectRes = [(name, gunanZhiName, int(ele['任职西历年']) if ele['任职西历年'] else None) for ele in selectRes]
				result.extend(selectRes)
		result = sorted(result, key = lambda x: x[2])
		df = pd.DataFrame(result, columns=['姓名', '官职名称', '任职西历年'])
		df.to_excel(writer, "姓同{}次".format(times), index=False)
		writer.save()


	df = pd.DataFrame(resMap.values(), columns=['次数', '姓名', '姓名比例', "官职名称+姓名", "官职名称+姓名比例", "姓名相同官职不同", "比例"])
	df.to_excel(writer, "Sheet1", index=False)
	writer.save()
	logger.info("running step2 done!")

def checkWrong(element):
	renZhiShiJian = element['任职西历年']
	shengNian     = element['生年']
	#"任职时间"-"生年"小于0或者大于90的情况
	if renZhiShiJian and shengNian:
		res = int(renZhiShiJian) - int(shengNian)
		if (res > 90) or (res < 0):
			return True  # "履历不正确"
		else:
			return False
	#没有出生时间，比较条件可以改为“任职朝代”在“时间1朝代”和“时间2朝代”之间就可以了 
	if not shengNian and renZhiShiJian:
		renZhiChaoDai =  element['任职朝代']
		chaoDai1 = element['时间1朝代']
		chaoDai2 = element['时间2朝代']
		if renZhiChaoDai in [chaoDai1, chaoDai2]:
			return False
		else:
			return True
	#没有任职时间，可能是重复出现的人，则用同一人其他任职时间的结果判断
	if not renZhiShiJian and shengNian:
		otherRenZhiShiJian = list(db['1980'].find({"官职类别": element['官职类别'], "姓名内编号": "1", "姓名": element['姓名']}))
		for x in otherRenZhiShiJian:
			renZhiShiJian = element['任职西历年']
			if renZhiShiJian and (int(renZhiShiJian) - int(shengNian) <= 90) and (int(renZhiShiJian) - int(shengNian) >= 0):
				return False
		return True
	return False

def checkWrong1(element):
	gunanZhiName = element['官职名称']
	renZhiZhiWei = element['任职职位']
	xingMing     = element['姓名']
	renZhiZhiWeis = [x['任职职位'] for x in db['data'].find({'姓名': xingMing, "姓名内编号": {"$nin" : ["", "0"]}})]
	if len(renZhiZhiWeis) == 0:
		return False
	return all([x.find(gunanZhiName) <0  for x in renZhiZhiWeis])

def doLvLiWrong(guanZhiLieBie):
	res = db['1980'].find({"官职类别": guanZhiLieBie, "姓名内编号": "1"})
	right = [x for x in res if not checkWrong1(x)]
	if right:
		db['right'].insert(right)
	return len(right)

def step3():
	db['right'].drop()
	total  = db['1980'].count()
	writer = pd.ExcelWriter(os.path.join(baseDir, '3.履历情况统计.xlsx'))

	gunanLeiBie  = list(db['1980'].distinct("官职类别"))
	res = []
	for element in gunanLeiBie:
		youLvLi   = db['1980'].find({"官职类别": element, "姓名内编号": "1"}).count()
		wuLvLi    = db['1980'].find({"官职类别": element, "姓名内编号": "0"}).count()
		rightLvLi = doLvLiWrong(element)
		#lvLiDaiDing = db['1980'].find({"官职类别": element, "姓名内编号": "1", "任职西历年": "", "生年": ""}).count()
		lvLiDaiDing = len([x for x in db['1980'].find({"官职类别": element, "姓名内编号": "1"}) if checkWrong1(x)])
		res.append([element, youLvLi, youLvLi/total, wuLvLi, wuLvLi/total, rightLvLi, rightLvLi/youLvLi, lvLiDaiDing, lvLiDaiDing/youLvLi])
	
	df = pd.DataFrame(res, columns=['官职类别', '有履历数量', "有履历比例", "无履历数量", "无履历比例", "履历正确", "履历正确比例", "履历待定", "履历待定比例"])
	df.to_excel(writer, "Sheet1", index=False)
	writer.save()

if __name__ == '__main__':
	#precondition()
	#step1()
	#step2()
	step3()
