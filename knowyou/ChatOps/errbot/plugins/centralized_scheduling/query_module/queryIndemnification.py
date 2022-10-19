from errbot import BotPlugin, webhook
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
#             	┆  ┏┓       ┏┓    ┆    
#             	┆ ┏┛┻━━━━━━━┛┻┓   ┆
#             	┆ ┃　　　　 　┃   ┆
#             	┆ ┃　 　━　 　┃   ┆
#             	┆ ┃　┳┛ 　┗┳　┃   ┆
#             	┆ ┃　　　　 　┃   ┆
#             	┆ ┃　 　┻　 　┃   ┆
#             	┆ ┗━┓　  　┏━━┛   ┆
#             	┆ 　┃　  　┃ 　   ┆　　　　　 
#             	┆ 　┃　  　┗━━━┓  ┆
#             	┆ 　┃　   　 　┣┓ ┆ 
#             	┆ 　┃　      　┏┛ ┆ 
#             	┆ 　┗┓┓┏━━┳┓┏━━┛  ┆ 
#             	┆  　┃┫┫　┃┫┫     ┆   
#             	┆  　┗┻┛　┗┻┛     ┆ 
#               /*神  兽  护  体 *\              

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

class QueryIndemnification(BotPlugin):

    @webhook('/queryRankOfSafeDaysInEachProvince/查询各省安全运行天数排名/')
    def queryRankOfSafeDaysInEachProvince(self, request):
        
        authorization_ip = config["ip_port"]["authorization_ip"]
        authorization_port = config["ip_port"]["authorization_port"]
        request_ip = config["ip_port"]["request_ip"]
        request_port = config["ip_port"]["request_port"]

        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type':'application/json;charset=UTF-8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        }
        headers['Authorization'] = getAccessToken(authorization_ip, authorization_port)

        url = 'http://' + request_ip + ':' + request_port + '/cdispatching/athousanddays/openplatform/safe/dashboard/list'
   
        self.log.debug(repr("The request is " + str(url)))
        self.log.debug(repr("The headers of request is " + str(headers)))

        response = requests.get(url, headers = headers)
        
        #self.log.debug(repr(response.text))
        data = response.json()
        code = data.get("code")
        self.log.debug(repr("The response code of this request is " + str(code)))
 
        if code == '000000':
            safeDaysResultBody = data.get("body")

            rank_id = 0
            step = 0
            safeDay = -1
            proD = getProvinceNameDictionary() 
          #  self.log.debug(repr(proD))

            finalResult = ''
 
            for oneProvinceSafeDayResult in safeDaysResultBody:
                step += 1
                if safeDay != oneProvinceSafeDayResult["safeDays"]:
                    rank_id += step
                    step = 0
                    safeDay = oneProvinceSafeDayResult["safeDays"]

            #    self.log.debug(repr("排名：" + str(rank_id)))
            #    self.log.debug(repr("省份：" + oneProvinceSafeDayResult["provinceCode"]))

                province = proD.get(oneProvinceSafeDayResult["provinceCode"])
                issueCount = oneProvinceSafeDayResult["issueCount"]
                finalResult += '排名：' + str(rank_id) + ', 省份：' + str(province) + ', 安全天：' + str(safeDay) + ', 问题数：' + str(issueCount) + ';<br>'

            return finalResult

        else:
            return "Query Failed! Code: " + str(code)   
