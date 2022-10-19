from chatterbot.logic import LogicAdapter
from chatterbot.conversation import Statement

from utils.basicUtil import *
from utils.readConfig import ReadConfig
from utils.handleTime import *
from utils.commonLogger import *

import requests

class QueryRankOfSafeDays(LogicAdapter):
    def __init__(self, chatbot, **kwargs):
        super().__init__(chatbot, **kwargs)

    def can_process(self, statement):
        if statement.text == '千天排名' or statement.text == '1000天排名' or statement.text == '一千天排名' or statement.text == '1千天排名' or statement.text == '各省安全运行天数排名' or statement.text == '各省排名':
            return True
        else:
            return False

    def process(self, statement, additional_response_selection_parameters=None):
        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type': 'application/json;charset=UTF-8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        }
        rc = ReadConfig()
        headers['Authorization'] = getAccessToken(rc.authorization_url)

        url = rc.query_url + '/cdispatching/athousanddays/openplatform/safe/dashboard/list'
        response = requests.get(url, headers=headers)

        logging.info('请求url：' + url)
        logging.info('请求响应状态码：' + str(response.status_code))

        if response.status_code == 200:
            confidence = 1
        else:
            confidence = 0

        data = response.json()
        code = data.get("code")

        if code == '000000':
            safeDaysResultBody = data.get("body")

            rank_id = 0
            step = 0
            safeDay = -1
            proD = getProvinceNameDictionary()

            finalResult = ''

            for oneProvinceSafeDayResult in safeDaysResultBody:
                step += 1
                if safeDay != oneProvinceSafeDayResult["safeDays"]:
                    rank_id += step
                    step = 0
                    safeDay = oneProvinceSafeDayResult["safeDays"]

                province = proD.get(oneProvinceSafeDayResult["provinceCode"])
                issueCount = oneProvinceSafeDayResult["issueCount"]
                finalResult += '排名：' + str(rank_id) + ', 省份：' + str(province) + ', 安全天：' + str(
                    safeDay) + ', 问题数：' + str(issueCount) + ';<br>'
        else:
            finalResult = "Query Failed! Code: " + str(code)

        response_statement = Statement(text=getResultTextFormat(finalResult))
        response_statement.confidence = confidence

        return response_statement
