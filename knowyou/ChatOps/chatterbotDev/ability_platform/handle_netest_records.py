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
    # 自动拨测结果：taskNamexx任务,拨测totalCntx次，成功succCntx次，失败(totalCnt-succCnt)x次，成功率succRatiox%
    if result == 1:
        code = 0
    else:
        code = 1

    if result_desc == "":
        msg = "成功，自动拨测结果：["
        for stat in stat_list:
            task_name = stat['taskName']
            total_cnt = stat['totalCnt']
            success_cnt = stat['succCnt']
            fail_cnt = total_cnt - success_cnt
            success_ratio = stat['succRatio']

            msg += f'{task_name}任务，拨测{total_cnt}次，成功{success_cnt}次，失败{fail_cnt}次，成功率{success_ratio}%；'
        msg += "]"
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
