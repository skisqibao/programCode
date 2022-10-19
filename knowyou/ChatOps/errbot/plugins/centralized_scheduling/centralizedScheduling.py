from errbot import BotPlugin, webhook
from werkzeug.routing import BaseConverter

from util_module.basic_util import *

import sys
import os
import time
import datetime 

import configparser
import demjson

################################################################################## *           \\ MIKU | SAMA //
#                                                                                # *_______________#########_______________________
#           The Webhook Service of Errbot                                        # *______________############_____________________
# Designed by                                                                    # *______________#############____________________
#     SongKun                                                                    # *_____________##__###########___________________
# Time                                                                           # *____________###__######_#####__________________
#      2021.12.31, Happy new year                                                # *____________###_#######___####_________________
# Describe                                                                       # *___________###__##########_####________________
#     The requests have been accomplished :                                      # *__________####__###########_####_______________
#       查询昨日告警                                                             # *________#####___###########__#####_____________
#       查询今日告警                                                             # *_______######___###_########___#####___________
#       查询昨日A类割接                                                          # *_______#####___###___########___######_________
#       查询今日A类割接                                                          # *______######___###__###########___######_______
#       查询各省安全运行天数排名/查询各省排名                                    # *_____######___####_##############__######______
#       查询各省严重隐患                                                         # *____#######__#####################_#######_____
#       查询河南严重隐患                                                         # *____#######__##############################____
#       查询浙江严重隐患                                                         # *___#######__######_#################_#######___
#                                                                                # *___#######__######_######_#########___######___
#                                                                                # *___#######____##__######___######_____######___
#                                                                                # *___#######________######____#####_____#####____
#                                                                                # *____######________#####_____#####_____####_____
#                                                                                # *_____#####________####______#####_____###______
#                                                                                # *______#####______;###________###______#________
#                                                                                # *________##_______####________####______________
#        Default                                                                 # *
##################################################################################
#           ◢◤◢◤
#　　　　　◢████◤         旅行者，向着星辰与深渊
#　　　⊙███████◤
#　●████████◤
#　　▼　　～◥███◤
#　　▲▃▃◢███　●　　●　　●　　●　　●　　●　　●　　●　      ◢◤
#　　　　　　███／█　／█　／█　／█　／█　／█　／█　／█　　◢◤
#　　　　　　█████████████████████████████████◤
#      ~ ~~~  ~~  ~~~~~~  ~~~~   ~~ ~~~ ~  ~~~  ~~~  ~~~ ~~~~~~~）））））———————
##################################################################################

# Read configuration file
dirname = os.path.split(os.path.realpath(__file__))[0]
filename = dirname + "/config.ini"

config = configparser.ConfigParser()
config.read(filename, encoding="utf-8")
webhook_url = config["ip_port"]["webhook_url"]

# Set parameters about time/date
currentDay = datetime.date.today()
oneDay = datetime.timedelta(days=1)
yesterday = currentDay - oneDay

yesterdayDate = yesterday.strftime("%Y-%m-%d")
yesterdayAt0Clock = yesterday.strftime("%Y-%m-%d 00:00:00")
yesterdayAt24Clock = yesterday.strftime("%Y-%m-%d 23:59:59")

currentDate = currentDay.strftime("%Y-%m-%d")
todayAt0Clock = time.strftime("%Y-%m-%d 00:00:00", time.localtime())
todayAtCurrentTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

# Content - trigger - method 
parameterDict = {
    "昨日告警":"queryAlertYesterday",
    "今日告警":"queryAlertToday",
    "昨日A类割接":"queryClassACutJoinYesterday",
    "今日A类割接":"queryClassACutJoinToday",
    "1000天排名":"queryRankOfSafeDaysInEachProvince",
    "各省排名":"queryRankOfSafeDaysInEachProvince",
    "各省严重隐患":"querySeriousHiddenDanger",
    "河南严重隐患":"queryHenanSeriousHiddenDanger",
    "浙江严重隐患":"queryZhejiangSeriousHiddenDanger",
    "投诉预警":"queryComplaintToday"
}

class RegexConverter(BaseConverter):
    def __init__(self, url_map, *args):
        super(RegexConverter, self).__init__(url_map)
        self.url = url_map
        self.regex = args[0]

    def to_python(self, value):
        return

# Request Internal Interface
def queryAlertYesterday():
    url = 'http://' + webhook_url + '/queryAlert/查询昨日告警/'
    return getResultTextFormat(requests.get(url).text)
def queryAlertToday():
    url = 'http://' + webhook_url + '/queryAlert/查询今日告警/'
    return getResultTextFormat(requests.get(url).text)
def queryClassACutJoinYesterday():
    url = 'http://' + webhook_url + '/queryClassACutJoin/查询昨日A类割接/'
    return getResultTextFormat(requests.get(url).text)
def queryClassACutJoinToday():
    url = 'http://' + webhook_url + '/queryClassACutJoin/查询今日A类割接/'
    return getResultTextFormat(requests.get(url).text)
def queryRankOfSafeDaysInEachProvince():
    url = 'http://' + webhook_url + '/queryRankOfSafeDaysInEachProvince/查询各省安全运行天数排名/'
    return getResultTextFormat(requests.get(url).text)
def querySeriousHiddenDanger():
    url = 'http://' + webhook_url + '/querySeriousHiddenDanger/查询各省严重隐患/'
    return getResultTextFormat(requests.get(url).text)
def queryHenanSeriousHiddenDanger():
    url = 'http://' + webhook_url + '/querySeriousHiddenDanger/查询河南严重隐患/'
    return getResultTextFormat(requests.get(url).text)
def queryZhejiangSeriousHiddenDanger():
    url = 'http://' + webhook_url + '/querySeriousHiddenDanger/查询浙江严重隐患/'
    return getResultTextFormat(requests.get(url).text)
def queryComplaintToday():
    url = 'http://' + webhook_url + '/queryAlert/查询今日投诉预警/'
    return getResultTextFormat(requests.get(url).text)



def queryDefault():
    answer = "您好，您这个问题我正在学习中, 等我学好了, 再回答您这个问题~~(*°▽°*)八(*°▽°*)~~"
    return getResultTextFormat(answer)


# External Interface Entrance
class CentralizedScheduling(BotPlugin):

    @webhook('/centralizedScheduling/<parameter>/')
    def CentralizedScheduling(self, request, parameter):
        self.log.debug(repr("本次请求参数：" + parameter))
        fun = parameterDict.get(parameter, "queryDefault")
        self.log.debug(repr("本次请求查询使用：" + fun + "方法"))

        finalResult = eval(fun)()
        self.log.debug(repr(finalResult))

        return finalResult

