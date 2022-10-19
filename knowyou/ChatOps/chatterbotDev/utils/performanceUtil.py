import json
import requests
import random

from utils.handleTime import extract_time_interval
from utils.readConfig import ReadConfig
from utils.commonLogger import *

dirname = os.path.split(os.path.realpath(__file__))[0]


def queryPerformance(time, type, wy, label_cn):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
        'content-type': 'application/json;charset=UTF-8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    }
    rc = ReadConfig()
    url = rc.query_url + '/cdispatching/monitor/v1/newscreen/alarminfo/queryNeKpiData'
    logging.info('请求url：' + url)

    dataStr = '[{"portName":"","interval":5,"levelType":"1","isDeviceDimension":0,"neId":"","isIOT":"1","bigRegion":"","provinceName":"","province":"","neName":"","neType":"","field":"","label":"","unit":"次数"}]'
    data = eval(dataStr)
    data[0].update(extract_time_interval(time.replace('-', '.')))

    # 对网元先做路由转码，然后基于base64加密
    from urllib import parse
    import base64
    data[0]['neName'] = str(base64.b64encode(parse.quote(wy).encode('utf-8')))[2:-1]

    for line in getLabelDictionaryList():
        if type == line['ne_type'] and label_cn == line['kpi_name']:
            data[0]['neType'] = line['ne_type']
            data[0]['label'] = line['kpi_name']
            data[0]['field'] = line['kpi_name_en']
            data[0]['interval'] = line['kpi_interval']
            data[0]['unit'] = line['kpi_unit']
            break

    if type != data[0]['neType']:
        return "未查询到相关指标字典详情"

    logging.info('请求参数：' + str(data))

    response = requests.post(url=url, data=json.dumps(data), headers=headers)
    logging.info('请求响应状态码：' + str(response.status_code))

    if response.status_code == 200:
        data = response.json()
        code = data.get("code")
        if code == '000000':
            todayY = data.get("body").get("result")[0].get("todayY")
            if len(todayY) > 0:
                cur_value = todayY[-1]
                dif_value = data.get("body").get("result")[0].get("difference")
                final_result = "查询到性能当前值：" + str(cur_value) + "，差异值：" + str(dif_value)
            else:
                final_result = "暂未查到数据"
        else:
            final_result = "查询获取失败"
    else:
        final_result = "请求失败"

    return final_result


def getLabelDictionaryList():
    labelDictList = []

    fileName = dirname + '/dictionary/performance_label.bcp'
    with open(fileName, 'r', encoding='UTF-8') as f:
        for line in f:
            (kpi_id, specialty, ne_type, kpi_name, kpi_name_en, kpi_interval, kpi_unit) = line.split('\t')
            labelDict = {}
            labelDict['kpi_id'] = kpi_id.strip()
            labelDict['specialty'] = specialty.strip()
            labelDict['ne_type'] = ne_type.strip()
            labelDict['kpi_name'] = kpi_name.strip()
            labelDict['kpi_name_en'] = kpi_name_en.strip()
            labelDict['kpi_interval'] = kpi_interval.strip()
            labelDict['kpi_unit'] = kpi_unit.strip()
            labelDictList.append(labelDict)

    return labelDictList


def getLabelList():
    labelList = set()
    fileName = dirname + '/dictionary/performance_label.bcp'
    with open(fileName, 'r', encoding='UTF-8') as f:
        for line in f:
            labelList.add(line.split('\t')[3])
    return labelList


def getTypeSet():
    typeSet = set()
    fileName = dirname + '/dictionary/performance_label.bcp'
    with open(fileName, 'r', encoding='UTF-8') as f:
        for line in f:
            typeSet.add(line.split('\t')[2])
    return typeSet


if __name__ == '__main__':
    # print(str(getTypeSet()))
    # print(str(getLabelDictionaryList()))
    # print(queryPerformance('2022-03-21', 'SAEGW', 'Tk5TQUVHVzcxQkhX', 'SGW承载容量峰值利用率'))
    print(queryPerformance('2022-03-21', 'SAEGW', 'NNSAEGW71BHW', 'SGW承载容量峰值利用率'))
    # print(queryPerformance('2022-03-21', 'SAEGW', 'Tk5TQUVHVzcxQkhX', 'SGi接口发送流量'))
    print(queryPerformance('2022-03-21', 'SAEGW', 'NNSAEGW71BHW', 'SGi接口发送流量'))
