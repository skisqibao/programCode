from errbot import BotPlugin, webhook
from werkzeug.routing import BaseConverter

from util_module.basic_util import *


import sys
import os
import time
import datetime 

import configparser
import demjson

######################################################
#               Query CutJoin Plugin                 #
# Designed by                                        #
#     SongKun                                        #
# Time                                               #
#     2021.12.31, Happy new year                     #
# Describe                                           #
#     The requests have been accomplished :          # 
#        查询昨日A类割接                             #
#        查询今日A类割接                             #
######################################################
#          　  . ,?iヽ..　　
#          　　ノ?,, ヽミ                  彡ミミ
#          (?,,／ ) 　ヽ?～—～′′ヾ?ミミミ彡
#          　　 　（ 　    　  ）        ）
# 你说你   　　　　(　、 ..）_＿彡( ,,.ノ        呢？
#          　　　　/／（ ?　　　 ?.ノ (
#          　 　　 //　　＼Ｙ?　.. 〆　.い
#          　　 （?　　　　　　　 //
#          　　 く?>　　　　　　 く?>
#      
    
dirname = os.path.split(os.path.realpath(__file__))[0]
filename = dirname + "/config.ini"

config = configparser.ConfigParser()
config.read(filename, encoding="utf-8")

currentDay = datetime.date.today()
oneDay = datetime.timedelta(days=1)
yesterday = currentDay - oneDay

yesterdayDate = yesterday.strftime("%Y-%m-%d")
yesterdayAt0Clock = yesterday.strftime("%Y-%m-%d 00:00:00")
yesterdayAt24Clock = yesterday.strftime("%Y-%m-%d 23:59:59")

currentDate = currentDay.strftime("%Y-%m-%d")
todayAt0Clock = time.strftime("%Y-%m-%d 00:00:00", time.localtime())
todayAtCurrentTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

class RegexConverter(BaseConverter):
    def __init__(self, url_map, *args):
        super(RegexConverter, self).__init__(url_map)
        self.url = url_map
        self.regex = args[0]

    def to_python(self, value):
        return

class QueryCutJoin(BotPlugin):

    @webhook('/queryClassACutJoin/查询昨日A类割接/')
    def queryClassACutJoinYesterday(self, request):
        
        authorization_ip = config["ip_port"]["authorization_ip"]
        authorization_port = config["ip_port"]["authorization_port"]
        request_ip = config["ip_port"]["request_ip"]
        request_port = config["ip_port"]["request_port"]

        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type':'application/json;charset=UTF-8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        headers['Authorization'] = getAccessToken(authorization_ip, authorization_port)

        url = 'http://' + request_ip + ':' + request_port + '/cdispatching/cutweb/cutlist/query'

        dataStr = '{"orders":[{"column":"cut_grade"}],"cutProvince":"","cutGrades":[1],"cutStatus":"","cutResult":"","isOvertime":"","isException":"","pageSize":200,"applicationId":2}'
        data = eval(dataStr)
   
        data['countTime'] = yesterdayDate

        self.log.debug(repr("The request is " + str(request)))
        self.log.debug(repr("The headers of request is " + str(headers)))
        self.log.debug(repr("The parameter type is " + str(type(data))))
        self.log.debug(repr("The parameter is " + str(data)))

        response = requests.post(url, headers = headers, data = demjson.encode(data))

        data = response.json()

        cutJoinResultBody = data.get("body")
        self.log.debug(repr(str(cutJoinResultBody)))
        self.log.debug(repr(str(type(cutJoinResultBody))))

        finalCutJoinResult = ""

        if cutJoinResultBody == None:
            finalCutJoinResult += "今日未查询到A类割接数据"
        else:
            cutJoinResultList = cutJoinResultBody["list"]

            for index in range(len(cutJoinResultList)):
                oneCutJoinResultDict = cutJoinResultList[index]
                finalCutJoinResult += str(index + 1) + ". " + oneCutJoinResultDict["cutNo"] + ";<br>"

        self.log.debug(repr(finalCutJoinResult))

        return finalCutJoinResult


    @webhook('/queryClassACutJoin/查询今日A类割接/')
    def queryClassACutJoinToday(self, request):
        authorization_ip = config["ip_port"]["authorization_ip"]
        authorization_port = config["ip_port"]["authorization_port"]
        request_ip = config["ip_port"]["request_ip"]
        request_port = config["ip_port"]["request_port"]

        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type':'application/json;charset=UTF-8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        headers['Authorization'] = getAccessToken(authorization_ip, authorization_port)

        url = 'http://' + request_ip + ':' + request_port + '/cdispatching/cutweb/cutlist/query'

        dataStr = '{"orders":[{"column":"cut_grade"}],"cutProvince":"","cutGrades":[1],"cutStatus":"","cutResult":"","isOvertime":"","isException":"","pageSize":200,"applicationId":2}'
        data = eval(dataStr)

        data['countTime'] = currentDate

        self.log.debug(repr("The request is " + str(request)))
        self.log.debug(repr("The headers of request is " + str(headers)))
        self.log.debug(repr("The parameter type is " + str(type(data))))
        self.log.debug(repr("The parameter is " + str(data)))

        response = requests.post(url, headers = headers, data = demjson.encode(data))

        data = response.json()

        cutJoinResultBody = data.get("body")
        self.log.debug(repr(str(cutJoinResultBody)))
        self.log.debug(repr(str(type(cutJoinResultBody))))

        finalCutJoinResult = ""
         
        if cutJoinResultBody == None:
            finalCutJoinResult += "今日未查询到A类割接数据/(._.`).<br>"
        else:
            cutJoinResultList = cutJoinResultBody["list"]
           
            for index in range(len(cutJoinResultList)):
                oneCutJoinResultDict = cutJoinResultList[index]
                finalCutJoinResult += str(index + 1) + ". " + oneCutJoinResultDict["cutNo"] + ";<br>"
          
        self.log.debug(repr(finalCutJoinResult))
        return finalCutJoinResult


    @webhook('/querySeriousHiddenDanger/查询各省严重隐患/')
    def querySeriousHiddenDanger(self, request):
        authorization_ip = config["ip_port"]["authorization_ip"]
        authorization_port = config["ip_port"]["authorization_port"]
        request_ip = config["ip_port"]["request_ip"]
        request_port = config["ip_port"]["request_port"]

        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type':'application/json;charset=UTF-8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        headers['Authorization'] = getAccessToken(authorization_ip, authorization_port)

        url = 'http://' + request_ip + ':' + request_port + '/cdispatching/netrisk/group/page/query'

        dataStr = '{"hiddenImportance": "严重", "problemProvinces": [], "pageNum": 1, "pageSize": 100}'
        data = eval(dataStr)

        self.log.debug(repr("The request is " + str(request)))
        self.log.debug(repr("The headers of request is " + str(headers)))
        self.log.debug(repr("The parameter type is " + str(type(data))))
        self.log.debug(repr("The parameter is " + str(data)))

        response = requests.post(url, headers = headers, data = demjson.encode(data))

        data = response.json()
        code = data.get("code")
        self.log.debug(repr("The response code of this request is " + str(code)))
 
        if code == '000000':
            seriousHiddenDangerResultList = data.get("body").get("list")

            finalResult = ''
 
            for oneSeriousHiddenDangerResult in seriousHiddenDangerResultList:
                finalResult += oneSeriousHiddenDangerResult["problemDesc"] + ';<br>'

            return finalResult

    @webhook('/querySeriousHiddenDanger/查询河南严重隐患/')
    def queryHenanSeriousHiddenDanger(self, request):
        authorization_ip = config["ip_port"]["authorization_ip"]
        authorization_port = config["ip_port"]["authorization_port"]
        request_ip = config["ip_port"]["request_ip"]
        request_port = config["ip_port"]["request_port"]

        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type':'application/json;charset=UTF-8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        headers['Authorization'] = getAccessToken(authorization_ip, authorization_port)

        url = 'http://' + request_ip + ':' + request_port + '/cdispatching/netrisk/group/page/query'

        dataStr = '{"hiddenImportance": "严重", "problemProvinces": ["河南"], "pageNum": 1, "pageSize": 100}'
        data = eval(dataStr)

        self.log.debug(repr("The request is " + str(request)))
        self.log.debug(repr("The headers of request is " + str(headers)))
        self.log.debug(repr("The parameter type is " + str(type(data))))
        self.log.debug(repr("The parameter is " + str(data)))

        response = requests.post(url, headers = headers, data = demjson.encode(data))

        data = response.json()
        code = data.get("code")
        self.log.debug(repr("The response code of this request is " + str(code)))

        if code == '000000':
            seriousHiddenDangerResultList = data.get("body").get("list")

            finalResult = ''

            for oneSeriousHiddenDangerResult in seriousHiddenDangerResultList:
                finalResult += oneSeriousHiddenDangerResult["problemDesc"] + ';<br>'

            return finalResult

    @webhook('/querySeriousHiddenDanger/查询浙江严重隐患/')
    def queryZhejiangSeriousHiddenDanger(self, request):
        authorization_ip = config["ip_port"]["authorization_ip"]
        authorization_port = config["ip_port"]["authorization_port"]
        request_ip = config["ip_port"]["request_ip"]
        request_port = config["ip_port"]["request_port"]

        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type':'application/json;charset=UTF-8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        headers['Authorization'] = getAccessToken(authorization_ip, authorization_port)

        url = 'http://' + request_ip + ':' + request_port + '/cdispatching/netrisk/group/page/query'

        dataStr = '{"hiddenImportance": "严重", "problemProvinces": [], "pageNum": 1, "pageSize": 100}'
        data = eval(dataStr)

        self.log.debug(repr("The request is " + str(request)))
        self.log.debug(repr("The headers of request is " + str(headers)))
        self.log.debug(repr("The parameter type is " + str(type(data))))
        self.log.debug(repr("The parameter is " + str(data)))

        response = requests.post(url, headers = headers, data = demjson.encode(data))

        data = response.json()
        code = data.get("code")
        self.log.debug(repr("The response code of this request is " + str(code)))

        if code == '000000':
            seriousHiddenDangerResultList = data.get("body").get("list")

            finalResult = ''

            for oneSeriousHiddenDangerResult in seriousHiddenDangerResultList:
                finalResult += oneSeriousHiddenDangerResult["problemDesc"] + ';<br>'

            return finalResult

