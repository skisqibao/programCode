from chatterbot.logic import LogicAdapter
from chatterbot.conversation import Statement

from utils.basicUtil import *
from utils.readConfig import ReadConfig
from utils.handleTime import *
from utils.commonLogger import *

import requests
import demjson
import cpca


class QuerySevenDaysWeather(LogicAdapter):
    def __init__(self, chatbot, **kwargs):
        super().__init__(chatbot, **kwargs)

    def can_process(self, statement):
        if statement.text.find('天气') != -1:
            return True
        else:
            return False

    def process(self, statement, additional_response_selection_parameters=None):
        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type': 'application/json;charset=UTF-8',
            'Accept': 'application/json',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        rc = ReadConfig()
        url = rc.query_url + '/cdispatching/monitor/v1/newscreen/weather/query/weatherInfo'
        dataStr = '{"weatherCity": "", "weatherNowDate": "", "weatherProvince": "" }'
        data = eval(dataStr)
        data['weatherNowDate'] = extract_date_day(statement.text, False)

        df = cpca.transform([statement.text])
        if df.loc[0][0]:
            data['weatherProvince'] = df.loc[0][0][0:2]
            if df.loc[0][1]:
                if df.loc[0][1].endswith('市'):
                    data['weatherCity'] = df.loc[0][1][0:2]

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
        final_result = ''

        if code == '000000':
            final_result = data['weatherProvince'] + data['weatherCity'] + '七日天气：<br>'
            weatherResultBody = res_data.get("body")

            if weatherResultBody:
                for weatherResult in weatherResultBody:
                    final_result += weatherResult["weatherDate"] + weatherResult["weatherStatus"] + "，高温：" + \
                                    weatherResult["weatherMaxtem"] + "，低温：" + weatherResult["weatherMintem"] + "，风向：" + weatherResult[
                                        "weatherWindAm"] + weatherResult["weatherWindScale"] + ";<br>"
            else:
                final_result += "暂未查询到相关天气数据"
        else:
            final_result += "查询失败，请检查或联系管理员"

        response_statement = Statement(text=final_result)
        response_statement.confidence = confidence

        return response_statement
