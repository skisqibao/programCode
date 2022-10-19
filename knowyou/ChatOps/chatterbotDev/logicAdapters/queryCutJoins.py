from chatterbot.logic import LogicAdapter
from chatterbot.conversation import Statement

from utils.readConfig import ReadConfig
from utils.basicUtil import *
from utils.handleTime import *
from utils.commonLogger import *

import requests
import demjson
import jieba.posseg as seg


class QueryCutJoinsAdapter(LogicAdapter):
    """
    使用前建议扩大数据库中statement表text字段的大小
    """

    def __init__(self, chatbot, **kwargs):
        super().__init__(chatbot, **kwargs)

    def can_process(self, statement):
        if '割接' in statement.text:
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
        url = rc.query_url + '/cdispatching/cutweb/cutlist/query'

        dataStr = '{"orders":[{"column":"cut_grade"}],"cutProvince":"","cutGrades":[1],"cutStatus":"","cutResult":"","isOvertime":"","isException":"","pageSize":200,"applicationId":2}'
        data = eval(dataStr)
        data['countTime'] = extract_date_day(statement.text, False)

        cut_list = []
        if statement.text.upper().find('A') != -1:
            cut_list.append(1)
        if statement.text.upper().find('B') != -1:
            cut_list.append(2)
        if statement.text.upper().find('C') != -1:
            cut_list.append(3)
        if statement.text.upper().find('D') != -1:
            cut_list.append(4)

        if len(cut_list) > 0:
            data["cutGrades"] = cut_list

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

        if code == 1:
            cutJoinResultBody = res_data.get("body")
            finalCutJoinResult = ""

            if cutJoinResultBody == None:
                finalCutJoinResult += "未查询到该类割接数据"
            else:
                cutJoinResultList = cutJoinResultBody["list"]

                for index in range(len(cutJoinResultList)):
                    oneCutJoinResultDict = cutJoinResultList[index]
                    finalCutJoinResult += str(index + 1) + ". " + oneCutJoinResultDict["cutNo"] + ";<br>"

            response_statement = Statement(text=getResultTextFormat(finalCutJoinResult))
        else:
            response_statement = Statement(text=getResultTextFormat('查询割接异常，请检查或联系相关人员检查。'))
        response_statement.confidence = confidence

        return response_statement


class QuerySeriousHiddenDanger(LogicAdapter):
    def __init__(self, chatbot, **kwargs):
        super().__init__(chatbot, **kwargs)

    def can_process(self, statement):
        if statement.text.find('严重隐患') != -1:
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
        url = rc.query_url + '/cdispatching/netrisk/group/page/query'

        dataStr = '{"hiddenImportance": "严重", "problemProvinces": [], "pageNum": 1, "pageSize": 100}'
        data = eval(dataStr)

        words = seg.cut(statement.text)
        province_list = []
        for k, v in words:
            if v == 'ns':
                if '省' in k:
                    province_list.append(k[:-1])
                else:
                    province_list.append(k)
        if len(province_list) > 0:
            data["problemProvinces"] = province_list

        response = requests.post(url, headers=headers, data=demjson.encode(data))

        logging.info('请求url：' + url)
        logging.info('请求parameters：' + str(data))
        logging.info('请求响应状态码：' + str(response.status_code))

        if response.status_code == 200:
            confidence = 1
        else:
            confidence = 0

        data = response.json()
        code = data.get("code")
        if code == '000000':
            finalResult = ''
            seriousHiddenDangerResultList = data.get("body").get("list")

            if seriousHiddenDangerResultList:
                for oneSeriousHiddenDangerResult in seriousHiddenDangerResultList:
                    finalResult += oneSeriousHiddenDangerResult["problemDesc"] + ';<br>'
            else:
                finalResult += '未查到严重隐患。'

            response_statement = Statement(text=getResultTextFormat(finalResult.replace('\n', '<br>')))
            response_statement.confidence = confidence

        return response_statement
