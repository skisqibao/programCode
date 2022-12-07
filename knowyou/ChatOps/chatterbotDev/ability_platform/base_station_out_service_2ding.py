import datetime
import json
from math import ceil
import requests

# get authorization information
def getAccessToken(authorization_url):
    headers = {'Authorization': 'Basic dGVuYW50X2ppdHVhbjpGbHp4M3FjMTIzQA=='}

    url =  authorization_url + '/cdispatching/user/oauth/token?grant_type=client_credentials'

    response = requests.post(url, headers=headers).json()

    access_token = response['access_token']
    token_type = response['token_type']

    authorization = token_type + " " +  access_token

    return authorization

class CutSpider(object):

    def __init__(self):
        self.headers = {
            "Authorization": "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX3Byb3ZpbmNlIjoiSlQiLCJ1c2VyX2lkIjoiNGMzNDZmOWEtYjM1YS00OTM5LWFmN2EtZDAyMTJjMjVjYTIxIiwiY2FsbF9uZXQiOiJXQU4iLCJ1c2VyX25hbWUiOiJtYXlvbmdxaWFuZyIsInNjb3BlIjpbImFsbCJdLCJ1c2VyX3JvbGVfbGV2ZWwiOjEsInN5c19wcm92aW5jZSI6IkpUIiwianRpIjoiYWYyZmJlZjItZmJmZC00Yzk5LWEyMGUtMTAwNGEzZjFlMWM5IiwiY2xpZW50X2lkIjoiY2xpZW50X3dlYiJ9.khkSaJzbnfkLMsyJBJmyYaCSOw1-bkgtgpx8ikpd8lo"
            ,
            "Cookie": "hyanalyse=%7B%22distinct_id%22%3A%2217fb46f7ab347f-0a3d7cdb06efeb-9771539-921600-17fb46f7ab467d%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22url%E7%9A%84domain%E8%A7%A3%E6%9E%90%E5%A4%B1%E8%B4%A5%22%2C%22%24latest_search_keyword%22%3A%22url%E7%9A%84domain%E8%A7%A3%E6%9E%90%E5%A4%B1%E8%B4%A5%22%2C%22%24latest_referrer%22%3A%22url%E7%9A%84domain%E8%A7%A3%E6%9E%90%E5%A4%B1%E8%B4%A5%22%7D%2C%22%24device_id%22%3A%2217fb46f7ab347f-0a3d7cdb06efeb-9771539-921600-17fb46f7ab467d%22%7D; JSESSIONID=941EC2C418A646E237EA53C13E65E56F"
            ,
            "Host": "117.132.184.21:8000", "Connection": "keep-alive",
            "Referer": "http://117.132.184.21:8000/test/cut/",
            "content-type": "application/json;charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36"
        }

    # 获取CUTid
    def get_guzhang(self, nowDay, province):
        data = json.dumps({"meetingId": "7080e1cbc7e5425c90708c7c0eb45a07", "interval": 15, "is3Day": 'false',
                           "region": {"province": province, "city": ""}, "time": nowDay})
        url = "http://117.132.184.21:8000/cdispatching/fault/v1/newscreen/getWithdrawalRepairCount"
        cutNo_json = requests.post(url=url, headers=self.headers, data=data)
        cutNo_json = cutNo_json.json()
        tfTotal_g5 = cutNo_json['body'][0]['tfTotal']
        tfTotal_g4 = cutNo_json['body'][1]['tfTotal']
        tfTotal_g2 = cutNo_json['body'][2]['tfTotal']
        tfTotal_olt = cutNo_json['body'][3]['tfTotal']
        # print(type(tfTotal_olt))
        list = [eval(tfTotal_g5), eval(tfTotal_g4), eval(tfTotal_g2), eval(tfTotal_olt)]
        return list

    def get_daping(self, province):
        Node = [["5G"], ["4G"], ["2G"], ["OLT"]]
        # global num
        num = []
        for node in Node:
            data = json.dumps(
                {"province": province, "city": "", "alarmStatus": 1, "meetingId": "7080e1cbc7e5425c90708c7c0eb45a07",
                 "signTypeList": node, "alarmReasonList": ["1", "2", "3", "4"], "startTime": "", "areaList": []})

            url = "http://117.132.184.21:8000/cdispatching/fault/v1/dpController/alarm"
            cutNo_json = requests.post(url=url, headers=self.headers, data=data)
            cutNo_json = cutNo_json.json()
            tfTotal = cutNo_json['body']
            if tfTotal == None:
                num.append(0)
            else:
                num.append(len(tfTotal))
        return num


class Index(CutSpider):
    def index(self):
        print(datetime.datetime.now())
        nowDay = datetime.datetime.now().strftime('%Y-%m-%d')
        provinces = ['新疆', '浙江', '天津', '四川', '陕西', '山西', '青海', '江苏', '吉林', '湖南', '黑龙江',
                     '西藏', '河南', '河北', '海南', '广东', '甘肃', '江西', '福建', '云南', '北京', '贵州',
                     '上海', '内蒙古', '广西', '山东', '宁夏', '辽宁', '湖北', '安徽', '重庆']
        content = '## Base station out of service ##\n '
        for province in provinces:
            guzhang_lst = self.get_guzhang(nowDay, province)  # 故障数据
            daping_lst = self.get_daping(province)  # 大屏数据

            # print(province, guzhang_lst, daping_lst)
            content += '\t- ' + province + str(guzhang_lst) + str(daping_lst) + ' \n\r'


        return content

def send_ding_msg(title, content, at_phone_list):
    webhook = "https://oapi.dingtalk.com/robot/send?access_token=79efa92ca54ecd2b7ea8ed9849974dde93aa0c7a945108fcc57d3cce068f92e6"

    data = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"{title}",
            "text": f"{content}"
        },
        "at": {
            "atMobiles": at_phone_list,
            "isAtAll": False
        }
    }
    resp = requests.post(webhook, json=data)
    resp.close()


if __name__ == '__main__':
    content = Index().index()

    at_phone_list = ['+86-15923008686', '+86-17603229691']
    for phone in at_phone_list:
        content += '@' + phone + '\n'

    print(content)
    send_ding_msg("基站退服", content, at_phone_list)
