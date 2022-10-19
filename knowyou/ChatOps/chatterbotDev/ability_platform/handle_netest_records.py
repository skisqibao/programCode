# -*- coding: utf-8 -*-

import os
import json
import sys
import requests
import io
import time

response = sys.argv[1]
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
with open(response, 'r', encoding='utf-8') as f:
    response = f.read()


def parser_data(response):
    response_data = json.loads(response)

    # item_count = response_data["itemCount"]
    # item_list = response_data["itemList"]
    result = response_data["result"]
    result_desc = response_data["resultDesc"]
    # stat_count = response_data["statCount"]
    stat_list = response_data["statList"]

    if result == 1:
        code = 0
    else:
        code = 1

    if result_desc == "":
        msg = "成功"
    else:
        msg = result_desc

    res = {
        "code": code,
        "msg": msg,
        "body": stat_list
    }

    return res


if __name__ == '__main__':
    response = '''
            {
          "itemCount": 10,
          "itemList": [
            {
              "cause": "成功",
              "success": 1,
              "taskId": "22091316500304082",
              "taskName": "NJ_gNB_HDB_AMF006_NJUPF005BHW_5G(VoNR)呼5G(VoNR)",
              "testTime": "2022-09-16 10:28:59"
            },
            {
              "cause": "成功",
              "success": 1,
              "taskId": "22091316500304082",
              "taskName": "NJ_gNB_HDB_AMF006_NJUPF005BHW_5G(VoNR)呼5G(VoNR)",
              "testTime": "2022-09-16 10:28:37"
            }
          ],
          "result": 1,
          "resultDesc": "",
          "statCount": 1,
          "statList": [
            {
              "avgDelay": 1575.4444444444443,
              "succCnt": 9,
              "succRatio": 90,
              "taskId": "22091316500304082",
              "taskName": "NJ_gNB_HDB_AMF006_NJUPF005BHW_5G(VoNR)呼5G(VoNR)",
              "totalCnt": 10
            }
          ]
        }
    '''

    res = parser_data(response)

    print(json.dumps(res, ensure_ascii=False))
