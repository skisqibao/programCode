from chatterbot.logic import LogicAdapter
from chatterbot.conversation import Statement

from utils.readConfig import ReadConfig
from utils.basicUtil import *
from utils.handleTime import *
from utils.commonLogger import *

import requests
import demjson

class QueryAlarmAdapter(LogicAdapter):
    """
    The custom logicAdapter of query-alarm-count which query with time parameter
    """

    def __init__(self, chatbot, **kwargs):
        super().__init__(chatbot, **kwargs)

    def can_process(self, statement):
        if statement.text.find('告警') != -1:
            return True
        else:
            return False

    def process(self, input_statement, additional_response_selection_parameters):
        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type': 'application/json;charset=UTF-8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        rc = ReadConfig()
        headers['Authorization'] = getAccessToken(rc.authorization_url)

        url = rc.query_url + '/cdispatching/fault/v1/alarmExport/getAlarmByCondition'

        data = {
            'meetingId': '4f18dc0908bb498db56e952e267e7c89',
            'pageNo': '1',
            'pageSize': '10',
            'provinceList': [],
            'cityList': [],
            'alarmMajor': []
        }
        data.update(extract_time_interval(input_statement.text))

        response = requests.post(url, headers=headers, data=demjson.encode(data))

        logging.info('请求url：' + url)
        logging.info('请求parameters：' + str(data))
        logging.info('请求响应状态码：' + str(response.status_code))

        if response.status_code == 200:
            confidence = 1
        else:
            confidence = 0

        res_data = response.json()
        code = res_data.get("code")

        if code == '000000':
            total_warnings = res_data["body"]["total"]
            response_statement = Statement(text='查询到告警总数：<b> {} </b>'.format(total_warnings))
        else:
            response_statement = Statement(text='查询告警异常，请检查或联系相关人员检查。')

        response_statement.confidence = confidence
        return response_statement


class QueryComplaintAdapter(LogicAdapter):
    """

    """

    def __init__(self, chatbot, **kwargs):
        super().__init__(chatbot, **kwargs)

    def can_process(self, statement):
        if statement.text.find('投诉') != -1 or statement.text.find('预警') != -1:
            return True
        else:
            return False

    def process(self, statement, additional_response_selection_parameters=None):
        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type': 'application/json;charset=UTF-8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        rc = ReadConfig()
        headers['Authorization'] = getAccessToken(rc.authorization_url)

        url = rc.query_url + '/cdispatching/fault/v1/alarmExport/getAlarmByCondition'

        data = {
            'meetingId': '4f18dc0908bb498db56e952e267e7c89',
            'pageNo': '1',
            'pageSize': '10',
            'provinceList': [],
            'cityList': [],
            'alarmMajor': ['投诉预警']
        }
        data.update(extract_time_interval(statement.text))

        response = requests.post(url, headers=headers, data=demjson.encode(data))

        if response.status_code == 200:
            confidence = 1
        else:
            confidence = 0

        res_data = response.json()
        code = res_data.get("code")

        finalResult = ''
        if code == '000000':
            alarmList = res_data["body"]["alarm"]

            if alarmList:
                for alarm in alarmList:
                    finalResult += alarm["alarmText"] + '<br>'
            else:
                finalResult += '投诉预警无数据。<br>'
        else:
            finalResult += 'Query Failed 。 CODE : ' + str(code)

        response_statement = Statement(text=getResultTextFormat(finalResult))
        response_statement.confidence = confidence

        return response_statement
