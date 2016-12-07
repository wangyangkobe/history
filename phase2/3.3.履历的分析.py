# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import unicode_literals

import logging
import pymongo
import pandas as pd
import sys, os

reload(sys)
sys.setdefaultencoding('utf8')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dbClient = pymongo.MongoClient()
db = dbClient.history

baseDir = os.path.join(os.path.dirname(__file__), "3_3_履历的分析")
logger.info("baseDir = {}".format(os.path.abspath(baseDir)))
if not os.path.exists(baseDir):
	os.mkdir(baseDir)

HEADER = "履历类别;履历编码;姓名内编号;时间1朝代;时间1年;时间1公历年;时间2朝代;时间2年;时间2公历年;任职方式;任职地;任职职位;任职职位简写;履历特点;职位总序号;官职类别-编码1;来源;页码;衙门;官职类别;官职类别（124）-编码1;地区;地区-编码2;官职名称;官职内-编码3;品级;姓名;又名;任职朝代;任职年;任职西历年;任职月;任职季;任职日;任职西历时间;任职前职位;任职类别;离职朝代;离职年;离职西历年;离职月;离职季;离职日;离职西历时间;离职原因;离职原因简写;备注;出处-personID;出处-人名权威资料;出处其他;生年;卒年;族;字;号;谥号;曾祖;祖父;父;兄1;兄2;兄3;兄4;兄5;兄6;兄7;兄8;兄9;兄10;兄11;子1;子2;子3;子4;子5;子6;子7;子8;子9;子10;子11;子12;子13;子14;子15;子16;孙1;孙2;孙3;孙4;孙5;孙6;孙7;孙8;孙9;孙10;孙11;孙12;孙13;孙14;孙15;孙16;民族;旗分;先赋;科举朝代;科年;科年公历;科举功名;世袭世职;其他出身;籍贯省;籍贯市;籍贯其他"

def getValues(element, fields):
	res = []
	for key in fields.split(";"):
		if key == "职位总序号":
			res.append(int(element[key]))
		else:
			res.append(element[key])
	return res
def calRenZhiTimeDiff(element1, element2):
	renZhiShiJian1 = element1[7] #'任职西历年'
	renZhiShiJian2 = element2[7]
	if element1[3] != element2[3]: #'官职名称''
		return ""
	if renZhiShiJian2 == '' or renZhiShiJian1 == '':
		return ""
	diff = int(renZhiShiJian2) - int(renZhiShiJian1)
	if diff == 0:
		return 0.5
	else:
		return diff

def step1():
	logger.info("running step1!")
	fields = "官职类别;官职类别-编码1;职位总序号;官职名称;姓名;任职朝代;任职年;任职西历年;民族;旗分;科举功名;世袭世职"
	writer = pd.ExcelWriter(os.path.join(baseDir, '1.任职时间间隔的计算.xlsx'))

	elements = list(db['1980'].find({"官职类别-编码1": "3"}).sort("职位总序号", 1))
	logger.info("the data items for 官职类别-编码1 = 3 is {}".format(len(elements)))
	values = [getValues(element, fields) for element in elements]
	values = sorted(values, key = lambda element: element[2])
	for index in range(len(values) - 1):
		current = values[index]
		next = values[index+1]
		current.append(calRenZhiTimeDiff(current, next))

	df = pd.DataFrame(values, columns=fields.split(";")+["任职时间间隔"])
	df.to_excel(writer,'官职类别-编码1 = 3',index=False)
	writer.save()

	elements = list(db['1980'].find({"官职类别-编码1": "5"}).sort("职位总序号", 1))
	logger.info("the data items for 官职类别-编码1 = 5 is {}".format(len(elements)))
	values = [getValues(element, fields) for element in elements]
	values = sorted(values, key = lambda element: element[2])
	for index in range(len(values) - 1):
		current = values[index]
		next = values[index+1]
		current.append(calRenZhiTimeDiff(current, next))

	df = pd.DataFrame(values, columns=fields.split(";")+["任职时间间隔"])
	df.to_excel(writer,'官职类别-编码1 = 5',index=False)
	writer.save()

	elements = list(db['1980'].find({"官职类别-编码1": {'$in' : ["1","2","4"]}}).sort("职位总序号", 1))
	logger.info("the data items for 官职类别-编码1 = 1 2 4 is {}".format(len(elements)))
	values = [getValues(element, fields) for element in elements]
	values = sorted(values, key = lambda element: element[2])
	for index in range(len(values) - 1):
		current = values[index]
		next = values[index+1]
		current.append(calRenZhiTimeDiff(current, next))

	df = pd.DataFrame(values, columns=fields.split(";")+["任职时间间隔"])
	df.to_excel(writer,'官职类别-编码1 = 1 2 4',index=False)
	writer.save()
	logger.info('finish step1!')

def checkWrong1(element):
	gunanZhiName = element['官职名称']
	renZhiZhiWei = element['任职职位']
	xingMing     = element['姓名']
	renZhiZhiWeis = [x['任职职位'] for x in db['data'].find({'姓名': xingMing, "姓名内编号": {"$nin" : ["", "0"]}})]
	if len(renZhiZhiWeis) == 0:
		return False
	return all([x.find(gunanZhiName) <0  for x in renZhiZhiWeis])

def step2():
	db['lvLiRight'].drop()
	logger.info("running step2!")
	fields = "姓名内编号;时间1朝代;时间1年;时间1公历年;时间2朝代;时间2年;时间2公历年;任职方式;任职地;任职职位;任职职位简写;职位总序号;官职类别;地区;官职名称;姓名;任职朝代;任职年;任职西历年;民族;旗分;科举功名;世袭世职"
	res = []
	for xingMing in db['right'].find({}).distinct("姓名"):
		for y in db['data'].find({"姓名": xingMing}, {'_id': False}):
			if not checkWrong1(y):
				res.append(y)		
	logger.info("data items in lvLiRight: {}".format(len(res)))
	db['lvLiRight'].insert(res)
	res = [getValues(element, fields) for element in res]

	df = pd.DataFrame(res, columns=fields.split(";"))
	writer = pd.ExcelWriter(os.path.join(baseDir, '2.履历正确数据库.xlsx'))
	df.to_excel(writer,'Sheet1',index=False)
	writer.save()

	logger.info('finish step2!')

def step3():
	logger.info("running step3!")
	writer = pd.ExcelWriter(os.path.join(baseDir, '3.任职职位简写.xlsx'))
	keJun  = db['right'].distinct(u"科举功名")
	jinShi = ["进士", "翻译进士"]
	juRen  = ["举人", "举人"]
	other  = list(set(keJun) - set(jinShi) - set(juRen) - set(["笔帖式", ""]))
	logger.info("其它文化身份: {}".format(";".join(other)))

	renZhiZhiWeiJianXie = db['right'].distinct(u"任职职位简写")
	guanZhiLeiBie =  db['right'].distinct(u"官职类别")

	for sheet in [jinShi, juRen, other]: 
		table = []
		for x in renZhiZhiWeiJianXie:
			row = [x]
			for y in guanZhiLeiBie:
				result = db['right'].find({'任职职位简写': x, '官职类别': y, '科举功名': {'$in': sheet}}).count()
				row.append(result)
			table.append(row)
		df = pd.DataFrame(table, columns=["任职职位简写"] + guanZhiLeiBie)
		sheetName = '_'.join(sheet) if len('_'.join(sheet)) < 31 else "其它文化身份"
		df.to_excel(writer,sheetName,index=False)
	writer.save()
	logger.info("finish step3!")

def step4():
	logger.info("running step4!")
	writer = pd.ExcelWriter(os.path.join(baseDir, '4.任职地.xlsx'))
	keJun  = db['right'].distinct(u"科举功名")
	jinShi = ["进士", "翻译进士"]
	juRen  = ["举人", "举人"]
	other  = list(set(keJun) - set(jinShi) - set(juRen) - set(["笔帖式", ""]))
	logger.info("其它文化身份: {}".format(";".join(other)))

	guanZhiLeiBie =  db['right'].distinct(u"官职类别")
	renZhiDi = db['right'].distinct(u"任职地")

	for sheet in [jinShi, juRen, other]: 
		table = []
		for x in renZhiDi:
			row = [x]
			for y in guanZhiLeiBie:
				result = db['right'].find({'任职地': x, '官职类别': y, '科举功名': {'$in': sheet}}).count()
				row.append(result)
			table.append(row)
		df = pd.DataFrame(table, columns=["任职地"] + guanZhiLeiBie)
		sheetName = '_'.join(sheet) if len('_'.join(sheet)) < 31 else "其它文化身份"
		df.to_excel(writer,sheetName,index=False)
	writer.save()
	logger.info("finish step4!")

def step5():
	logger.info("running step5!")
	writer = pd.ExcelWriter(os.path.join(baseDir, '5.历任履历中最初的任职职位.xlsx'))
	fields = "任职地;任职职位;任职职位简写;官职类别;官职名称;姓名;任职西历年;科举功名"

	for lvLiLeiBie in ["1", "2", "3"]:
		table = []
		for element in db['right'].find({}):
			result = list(db['data'].find({'姓名': element['姓名'], '职位总序号': element['职位总序号'], '履历类别': lvLiLeiBie}))
			result = sorted(result, key = lambda x: int(x['姓名内编号']))
			first = None
			for x in result:
				if not x['任职职位简写']:
					first = x
					break
			if first:
				table.append(getValues(first, fields))
			else:
				table.append(["", "", "", element['官职类别'], element['官职名称'], element['姓名'], element['任职西历年'], element['科举功名']])

		df = pd.DataFrame(table, columns=fields.split(";"))
		df.to_excel(writer,"履历类别{}".format(lvLiLeiBie),index=False)
	writer.save()	

	logger.info("finish step5!")

def step6():
	logger.info("running step6!")
	writer = pd.ExcelWriter(os.path.join(baseDir, '6.历任履历中最后的任职职位.xlsx'))
	fields = "任职地;任职职位;任职职位简写;官职类别;官职名称;姓名;任职西历年;科举功名"

	for lvLiLeiBie in ["1", "2", "3"]:
		table = []
		for element in db['right'].find({}):
			result = list(db['data'].find({'姓名': element['姓名'], '职位总序号': element['职位总序号'], '履历类别': lvLiLeiBie}))
			result = sorted(result, key = lambda x: int(x['姓名内编号']))
			last = None
			for x in reversed(result):
				if x['任职职位简写']:
					last = x
					break
			if last:
				table.append(getValues(last, fields))
			else:
				table.append(["", "", "", element['官职类别'], element['官职名称'], element['姓名'], element['任职西历年'], element['科举功名']])

		df = pd.DataFrame(table, columns=fields.split(";"))
		df.to_excel(writer,"履历类别{}".format(lvLiLeiBie),index=False)
	writer.save()	

	logger.info("finish step6!")

if __name__ == '__main__':
	#step1()
	#step2()
	#step3()
	#step4()
	#step5()
	step6()