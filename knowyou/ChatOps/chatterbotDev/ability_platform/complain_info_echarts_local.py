# -*- coding: utf-8 -*-

from pyecharts import options as opts
from pyecharts.charts import Line
from pyecharts.render import make_snapshot

import os
import json
import sys
import requests
import io
import time

from snapshot_phantomjs import snapshot

# response = sys.argv[1]
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
#
# with open(response, 'r', encoding='utf-8') as f:
#     response = f.read()

file_path = "{}/".format(os.path.dirname(os.path.abspath(__file__)))
# jiangsu
# ip_port = "http://10.198.40.176:8000"
# local
ip_port = "http://117.132.184.21:8000"


# 解析数据实体类
class ComplainInfo:
    bus_name = ''
    region = ''
    chart_x = []
    chart_num = []
    chart_daily = []

    def __init__(self, bus_name, region, chart_x, chart_num, chart_daily):
        self.bus_name = bus_name
        self.region = region
        self.chart_x = chart_x
        self.chart_num = chart_num
        self.chart_daily = chart_daily


# 鉴权接口
def getAccessToken():
    headers = {'Content-Type': 'application/json'}
    url = ip_port + "/cdispatching/user/obtain/token"
    data = {
        "systemid": "liuzhiqiang_wlw",
        "systemkey": "Pass@1995",
    }
    response = requests.post(url=url, data=json.dumps(data), headers=headers)
    res_json = response.json()

    if '0' == res_json['Code']:
        token = res_json['AccessToken']
        token_header = {
            "Authorization": 'bearer ' + token
        }
    else:
        print("鉴权接口 请求失败, Code: " + res_json['Code'])

    return token_header


# 解析投诉详情接口响应
def parser_data(response):
    response_data = json.loads(response)
    meeting_id = response_data['body']['meetingId']

    headers = getAccessToken()
    url = ip_port + '/cdispatching/fault/v1/dpController/getComplainInfo?meetingId=' + meeting_id + '&beginTime=&endTime='

    responses = requests.get(url, headers=headers)
    print(responses.text)

    response_data = responses.json()

    body_15 = response_data['body']['15分钟']

    info_list = []
    for data_15 in body_15:
        bus_name = data_15['busName']
        region = data_15['region']
        chart_daily = data_15['chartDaily']
        chart_num = data_15['chartNum']
        chart_x = data_15['chartX']

        if chart_x == None:
            continue

        info = ComplainInfo(bus_name, region, chart_x, chart_num, chart_daily)
        info_list.append(info)

    return info_list


# 作图
def line_chart(subtitle, x, y1, y2) -> Line:
    width = "760px"
    height = "400px"

    c = (
        Line(init_opts=opts.InitOpts(width=width, height=height, bg_color="white", js_host=file_path))
            .add_xaxis(x)
            .add_yaxis(
            '',
            y1,
            is_smooth=True,
            is_symbol_show=False,
            color='#CDE6F9',
            areastyle_opts=opts.AreaStyleOpts(opacity=0.5, color='#CDE6F9'),
        )
            .add_yaxis(
            '',
            y2,
            is_smooth=True,
            is_symbol_show=False,
            color='#9EDAF8',
            areastyle_opts=opts.AreaStyleOpts(opacity=0.5, color='#9EDAF8'),
        )
            .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
            .set_global_opts(
            title_opts=opts.TitleOpts(
                title="投诉趋势图",
                subtitle=subtitle,
                pos_top='top',
                item_gap=2,
                title_textstyle_opts=(opts.TextStyleOpts(color='black', font_weight='bold', font_size='14')),
                subtitle_textstyle_opts=(opts.TextStyleOpts(color='black', font_weight='lighter', font_size='11')),
            ),
            xaxis_opts=opts.AxisOpts(
                type_='category',
                boundary_gap=True,
            ),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            legend_opts=opts.LegendOpts(is_show=False),
        )
    )
    return c


# 上传接口
def upload(image_path):
    headers = getAccessToken()
    url = ip_port + "/cdispatching/file/v1/ftp/upload"
    file = {
        "sample_file": open(image_path, "rb")
    }
    response = requests.post(url=url, files=file, headers=headers)
    res_json = response.json()
    if "000000" == res_json['code']:
        attachment_id = res_json['body'][0]['attachmentId']
        attachment_name = res_json['body'][0]['attachmentName']
    else:
        print("上传接口 请求失败")

    res = {"attachment_id": attachment_id, "attachment_name": attachment_name}

    return res


# 下载接口

if __name__ == '__main__':

    response = '''
        {
            "code": "000000",
            "message": "SUCCESS",
            "time": "2022-09-14T16:10:13.028+0800",
            "body": {
              "createTime": "2022-09-14 08:09:25",
              "dispatchTime": "",
              "provinceNames": [],
              "timeLimit": "2022-9-14 16:09:38",
              "neNames": [],
              "meetingId": "4d8b6cdeec4f4331be318f053d45e6c1"
            }
        }
    '''

    info_list = parser_data(response)
    i = 0
    image_list = []

    # 生成图片
    for info in info_list:
        subtitle = info.bus_name
        region = info.region
        x = info.chart_x
        y1 = info.chart_num
        y2 = info.chart_daily
        image_path = os.path.join(file_path, 'complain_image')
        image_name = os.path.join(image_path, 'image' + str(i) + '.png')

        make_snapshot(snapshot, line_chart(subtitle, x, y1, y2).render(), image_name)

        i += 1
        image_list.append(image_name)

    # 上传图片
    id_list = []
    for image in image_list:
        # print(image)
        res_dict = upload(image)
        # print(attachment_id)
        # prefix = ip_port + "/cdispatching/file/v1/ftp/download/"
        # id_list.append(prefix + attachment_id)
        id_list.append(res_dict)

    url_dict = {"type": "image", "urls": id_list}
    print(json.dumps(url_dict))
