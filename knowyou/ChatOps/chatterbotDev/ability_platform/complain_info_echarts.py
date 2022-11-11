# -*- coding: utf-8 -*-

from pyecharts import options as opts
from pyecharts.charts import Line
from pyecharts.render import make_snapshot
import PIL.Image as Image

import os
import json
import sys
import requests
import io
import time

from snapshot_phantomjs import snapshot

response = sys.argv[1]
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

with open(response, 'r', encoding='utf-8') as f:
    response = f.read()

file_path = "{}/".format(os.path.dirname(os.path.abspath(__file__)))
image_path = os.path.join(file_path, 'complain_image')
# jiangsu
ip_port = "http://10.198.40.176:8000"
# local
# ip_port = "http://117.132.184.21:8000"

# 解析数据实体类
class ComplainInfo:
    _type = ''
    bus_name = ''
    region = ''
    chart_x = []
    chart_num = []
    chart_daily = []

    def __init__(self, _type, bus_name, region, chart_x, chart_num, chart_daily):
        self._type = _type
        self.bus_name = bus_name
        self.region = region
        self.chart_x = chart_x
        self.chart_num = chart_num
        self.chart_daily = chart_daily

    def __str__(self):
        str = f'{self.bus_name}--{self._type}--{self.region} : [{self.chart_daily}]'
        return str


# 鉴权接口
def get_access_token():
    headers = {'Content-Type': 'application/json'}
    url = ip_port + "/cdispatching/user/obtain/token"
    data = {
        "systemid": "liuzhiqiang",
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
    meeting_id = response_data['local']['meetingId']

    headers = get_access_token()
    url = ip_port + '/cdispatching/fault/v1/dpController/getComplainInfo?meetingId=' + meeting_id + '&beginTime=&endTime='

    responses = requests.get(url, headers=headers)

    response_data = responses.json()

    info_list = []

    if '15分钟' in response_data['body']:
        body_15 = response_data['body']['15分钟']

        for data_15 in body_15:
            bus_name = data_15['busName']
            region = data_15['region']
            chart_daily = data_15['chartDaily']
            chart_num = data_15['chartNum']
            chart_x = data_15['chartX']

            if chart_x == None:
                continue

            info = ComplainInfo('15分钟', bus_name, region, chart_x, chart_num, chart_daily)
            info_list.append(info)

    if '日累计' in response_data['body']:
        body_30 = response_data['body']['日累计']

        for data_30 in body_30:
            bus_name = data_30['busName']
            region = data_30['region']
            chart_daily = data_30['chartDaily']
            chart_num = data_30['chartNum']
            chart_x = data_30['chartX']

            if chart_x == None:
                continue

            info = ComplainInfo('日累计', bus_name, region, chart_x, chart_num, chart_daily)
            info_list.append(info)

    if '一小时' in response_data['body']:
        body_60 = response_data['body']['一小时']

        for data_60 in body_60:
            bus_name = data_60['busName']
            region = data_60['region']
            chart_daily = data_60['chartDaily']
            chart_num = data_60['chartNum']
            chart_x = data_60['chartX']

            if chart_x == None:
                continue

            info = ComplainInfo('一小时', bus_name, region, chart_x, chart_num, chart_daily)
            info_list.append(info)

    return info_list


# 作图
def line_chart(_type, region, subtitle, x, y1, y2) -> Line:
    width = "760px"
    height = "400px"

    c = (
        Line(init_opts=opts.InitOpts(width=width, height=height, bg_color="white", js_host=file_path))
            .add_xaxis(x)
            .add_yaxis(
            '投诉量',
            y1,
            is_smooth=True,
            is_symbol_show=False,
            color='#2F4554',
            areastyle_opts=opts.AreaStyleOpts(opacity=0.5, color='#CDE6F9'),
        )
            .add_yaxis(
            '日常值',
            y2,
            is_smooth=True,
            is_symbol_show=False,
            color='#C23531',
            areastyle_opts=opts.AreaStyleOpts(opacity=0.5, color='#9EDAF8'),
        )
            .set_series_opts(label_opts=opts.LabelOpts(is_show=True))
            .set_global_opts(
            title_opts=opts.TitleOpts(
                title="投诉趋势图" + f'--{_type}',
                subtitle=subtitle + f'({region})',
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
            legend_opts=opts.LegendOpts(is_show=True, legend_icon='roundRect'),
        )
    )
    return c


# 生成图片
def generate_image(info, i):
    _type = info._type
    subtitle = info.bus_name
    region = info.region
    x = info.chart_x
    y1 = info.chart_num
    y2 = info.chart_daily

    image_name = os.path.join(image_path, 'image' + str(i) + '.png')

    make_snapshot(snapshot, line_chart(_type, region, subtitle, x, y1, y2).render(), image_name)

    return image_name


# 图像拼接
def merge_image(image_list, final_path):
    image_size = 1000  # 每张小图片的大小
    width = 760
    height = 400
    image_row = len(image_list)  # 图片间隔，也就是合并成一张图后，一共有几行
    image_column = 1  # 图片间隔，也就是合并成一张图后，一共有几列

    to_image = Image.new('RGB', (image_column * width, image_row * height))  # 创建一个新图
    # 循环遍历，把每张图片按顺序粘贴到对应位置上
    for y in range(1, image_row + 1):
        for x in range(1, image_column + 1):
            from_image = Image.open(image_list[image_column * (y - 1) + x - 1]) \
                .resize((width, height), Image.ANTIALIAS)
            to_image.paste(from_image, ((x - 1) * width, (y - 1) * height))
    return to_image.save(final_path)  # 保存新图


# 上传接口
def upload(image_path):
    headers = get_access_token()
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
    # response = '''
    # {"code":"000000","message":"SUCCESS","time":"2022-09-08T10:08:49.448+0800","body":{"15分钟":[{"busName":"手机上网（4G）","busNum":"9","increment":"5","dailyVal":"4","interval":"15分钟","region":"江苏","chartX":["09:05","09:10","09:15","09:20","09:25","09:30","09:35","09:40","09:45","09:50","09:55","10:00","10:05"],"chartNum":["9","7","7","8","9","6","3","2","3","3","4","7","9"],"chartDaily":["4","5","4","4","4","5","5","5","5","5","4","4","4"]},{"busName":"VOLTE","busNum":"","increment":"","dailyVal":"0","interval":"15分钟","region":"江苏","chartX":["09:05","09:10","09:15","09:20","09:25","09:30","09:35","09:40","09:45","09:50","09:55","10:00","10:05"],"chartNum":["0","0","0","0","0","0","0","0","0","0","0","0","0"],"chartDaily":["0","0","0","0","0","0","0","0","0","0","0","0","0"]}]}}
    # '''

    # response ='''
    # {"15分钟":[{"busName":"手机上网（4G）","busNum":"4","increment":"1","dailyVal":"3","interval":"15分钟","region":"江苏","chartX":["16:15","16:20","16:25","16:30","16:35","16:40","16:45","16:50","16:55","17:00","17:05","17:10","17:15"],"chartNum":["4","4","4","4","5","2","2","1","2","1","1","0","0"],"chartDaily":["3","3","3","3","3","3","3","3","3","3","4","4","3"]},{"busName":"VOLTE","busNum":"0","increment":"-1","dailyVal":"1","interval":"15分钟","region":"江苏","chartX":["16:15","16:20","16:25","16:30","16:35","16:40","16:45","16:50","16:55","17:00","17:05","17:10","17:15"],"chartNum":["0","0","0","0","0","0","0","0","0","0","0","0","0"],"chartDaily":["1","1","1","0","1","1","1","1","1","0","0","1","1"]}]}
    # '''

    t1 = time.time()
    info_list = parser_data(response)
    t2 = time.time()
    i = 0
    image_list = []

    # 生成图片
    for info in info_list:
        _type = info._type
        subtitle = info.bus_name
        region = info.region
        x = info.chart_x
        y1 = info.chart_num
        y2 = info.chart_daily
        image_path = os.path.join(file_path, 'complain_image')
        image_name = os.path.join(image_path, 'image' + str(i) + '.png')

        make_snapshot(snapshot, line_chart(_type, region, subtitle, x, y1, y2).render(), image_name)

        i += 1
        image_list.append(image_name)

#    from concurrent.futures import ThreadPoolExecutor, as_completed

#    with ThreadPoolExecutor() as pool:
#        futures = [pool.submit(generate_image, info_list[i], i) for i in range(len(info_list))]
        # image_task = pool.map(generate_image, (info_list[i], i) for i in range(info_list))
#        for future in as_completed(futures):
#            image_list.append(future.result())
    t3 = time.time()

    final_path = os.path.join(image_path, 'final_image' + '.png')
    merge_image(image_list, final_path)
    res_dict = upload(final_path)
    url_dict = {"type": "image", "urls": [res_dict], "final_time": info_list[-1].chart_x[-1]}
    # # 上传图片
    # id_list = []
    # for image in image_list:
    #     # print(image)
    #     res_dict = upload(image)
    #     # print(attachment_id)
    #     # prefix = ip_port + "/cdispatching/file/v1/ftp/download/"
    #     # id_list.append(prefix + attachment_id)
    #     id_list.append(res_dict)
    # t4 = time.time()
    # url_dict = {"type": "image", "urls": id_list}

    # print("parser         use : ",t2-t1)
    # print("generate image use : ",t3-t2)
    # print("load     image use : ",t4-t3)
    print(json.dumps(url_dict))
