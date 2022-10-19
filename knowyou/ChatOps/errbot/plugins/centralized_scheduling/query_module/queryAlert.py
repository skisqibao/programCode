from errbot import BotPlugin, webhook
from util_module.basic_util import *

import sys
import os
import time
import datetime 

import configparser
import demjson

######################################################
#               Query Alert Plugin                   #
# Designed by                                        #
#     SongKun                                        #
# Time                                               #
#     2021.12.31, Happy new year                     #
# Describe                                           #
#     The requests have been accomplished :          # 
#        查询昨日告警                                #
#        查询今日告警                                #
######################################################
#  国际通用手势 ———————     /´¯)
#               ——————     /—-/
#               —————     /—-/
#               ————     /—-/
#               ——— –/´¯/’–’/´¯`·_
#               ———-/’/–/—-/—–/¨¯
#               ——–(’(———- ¯~/’–’)
#               —— —\————————-’—–/
#               —— —-’’——————_-·´
#               ——  ——\——————–(
#               ———  —-\—————-- 小小中指，不成敬意。
#

#dirname = sys.path[0]
#dirname = os.path.realpath(__file__)
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

class QueryAlert(BotPlugin):

    @webhook('/queryAlert/查询昨日告警/')
    def queryAlertYesterday(self, request):
        
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

        url = 'http://' + request_ip + ':' + request_port + '/cdispatching/fault/v1/alarmExport/getAlarmByCondition'

        data = {
            'meetingId':'4f18dc0908bb498db56e952e267e7c89',
            'pageNo':'1',
            'pageSize':'10',
            'provinceList':[],
            'cityList':[],
            'alarmMajor':[]
        }

        data['startTime'] = yesterdayAt0Clock
        data['endTime'] = yesterdayAt24Clock

#        headers.update(eval(headers1.replace('\"','\'').replace('“','\'').replace('”','\'')))
#        data.update(eval(data1.replace('\"','\'').replace('“','\'').replace('”','\'')))

        self.log.debug(repr(request))
        self.log.debug(repr(headers))
        self.log.debug(repr(data))

        response = requests.post(url, headers = headers, data = demjson.encode(data))
        data = response.json()
        total_warnings = data["body"]["total"]

        return "昨日告警总数：<b>" + str(total_warnings) + "</b>"


    @webhook('/queryAlert/查询今日告警/')
    def queryAlertToday(self, request):

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

        url = 'http://' + request_ip + ':' + request_port + '/cdispatching/fault/v1/alarmExport/getAlarmByCondition'

        data = {
            'meetingId':'4f18dc0908bb498db56e952e267e7c89',
            'pageNo':'1',
            'pageSize':'10',
            'provinceList':[],
            'cityList':[],
            'alarmMajor':[]
        }

        data['startTime'] = todayAt0Clock
        data['endTime'] = todayAtCurrentTime

        self.log.debug(repr(request))
        self.log.debug(repr(headers))
        self.log.debug(repr(data))

        response = requests.post(url, headers = headers, data = demjson.encode(data))
        data = response.json()
        total_warnings = data["body"]["total"]

        return "今日告警总数：<b>" + str(total_warnings) + "</b>"

    @webhook('/queryAlert/查询今日投诉预警/')
    def queryComplaintToday(self, request):

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

        url = 'http://' + request_ip + ':' + request_port + '/cdispatching/fault/v1/alarmExport/getAlarmByCondition'

        data = {
            'meetingId':'4f18dc0908bb498db56e952e267e7c89',
            'pageNo':'1',
            'pageSize':'10',
            'provinceList':[],
            'cityList':[],
            'alarmMajor':['投诉预警']
        }

        data['startTime'] = yesterdayAt0Clock 
        data['endTime'] = todayAtCurrentTime

        self.log.debug(repr(request))
        self.log.debug(repr(headers))
        self.log.debug(repr(data))

        response = requests.post(url, headers = headers, data = demjson.encode(data))
        data = response.json()
        code = data.get("code")
  
        finalResult = ''
        if code == "000000":
            alarmList = data["body"]["alarm"]

            if alarmList: 
                for alarm in alarmList:
                    finalResult += alarm["alarmText"] + '<br>'
            else:
                finalResult += '今日投诉预警无数据。<br>'
        else:
           finalResult += 'Query Failed 。 CODE : ' + str(code) 
        return finalResult


    @webhook('/queryTest/')
    def queryTest(self, request):
        self.log.debug(dirname)
        self.log.debug(filename)

        self.log.debug(time.localtime())
        self.log.debug(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        self.log.debug(time.strftime("%Y-%m-%d 00:00:00", time.localtime()))

        self.log.debug(datetime.date.today())
        self.log.debug(datetime.timedelta(days=1))

        self.log.debug("yesterdayAt0Clock : " + yesterdayAt0Clock)
        self.log.debug("yesterdayAt24Clock : " + yesterdayAt24Clock)
        self.log.debug("todayAt0Clock : " + todayAt0Clock)
        self.log.debug("todayAtCurrentTime : " + todayAtCurrentTime)
        return repr(config)
