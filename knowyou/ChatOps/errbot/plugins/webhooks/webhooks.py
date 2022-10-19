from errbot import BotPlugin, webhook
import json
import requests
import demjson

class Webhooks(BotPlugin):
    @webhook
    def test(self, request):
       self.log.debug(repr(request))
       return "This is OK, your test success.\n"

    @webhook
    def 你好(self, request):
       self.log.debug(repr(request))
       return "你也好\n"

    @webhook('/example/<name>/<action>')
    def example0(self, request, name, action):
       self.log.debug(repr(request))
       return "你好, %s 时候差不多了，该去 %s了.\n" % (name, action)

    @webhook('/alert0/')
    def alert0(self, request ):
        headers = {
            'Connection': 'keep-alive',
            'Authorization': 'bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6WyJhbGwiXSwianRpIjoiMGViZjZlZTktNTQyZi00MWIyLThkZTUtNDJhZTQzMTJhNTNkIiwiY2xpZW50X2lkIjoidGVuYW50X2ppdHVhbiJ9.0xUJuM0NBbEZitzzFvV02VWyRszXgxCcFAWgyEMy71I',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type':'application/json;charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'Origin': 'http://117.132.184.21:8000',
            'Referer': 'http://117.132.184.21:8000//test/fault/index_fault.html', 
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cookie': 'hyanalyse=%7B%22distinct_id%22%3A%2217a7e76418e4cb-0b8e7d20e781a3-52193f12-2073600-17a7e76418fada%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22url%E7%9A%84domain%E8%A7%A3%E6%9E%90%E5%A4%B1%E8%B4%A5%22%2C%22%24latest_search_keyword%22%3A%22url%E7%9A%84domain%E8%A7%A3%E6%9E%90%E5%A4%B1%E8%B4%A5%22%2C%22%24latest_referrer%22%3A%22url%E7%9A%84domain%E8%A7%A3%E6%9E%90%E5%A4%B1%E8%B4%A5%22%7D%2C%22%24device_id%22%3A%2217a7e76418e4cb-0b8e7d20e781a3-52193f12-2073600-17a7e76418fada%22%7D; JSESSIONID=FD5F6CDC71F19236E2E4B7F509838128'
        }
        url = 'http://117.132.184.21:8000/cdispatching/fault/v1/alarmExport/getAlarmByCondition'
        data = {
            'meetingId':'4f18dc0908bb498db56e952e267e7c89',
            'pageNo':'1',
            'pageSize':'10',
            'startTime':'2021-11-22 15:20:01',
            'endTime':'2021-11-23 15:20:01',
            'provinceList':[],
            'cityList':[],
            'alarmMajor':[]
        }

        response = requests.post(url, headers = headers, data = demjson.encode(data))

        #response.encoding = 'utf-8'

        return response.text


    def getAccessToken(self):
        headers = {'Authorization': 'Basic dGVuYW50X2ppdHVhbjpGbHp4M3FjMTIzQA=='}
        url = 'http://117.132.184.21:8000/cdispatching/user/oauth/token?grant_type=client_credentials'

        response = requests.post(url, headers=headers).json()
        
        self.log.debug(repr(response))

        access_token = response['access_token']
        token_type = response['token_type']
  
        authorization = token_type + " " +  access_token

        self.log.debug("The current authorization of header is " + authorization)

        return authorization

    @webhook('/alert1/<headers1>/<data1>/')
    def alert1(self, request, headers1, data1):

        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type':'application/json;charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'Origin': 'http://117.132.184.21:8000',
            'Referer': 'http://117.132.184.21:8000//test/fault/index_fault.html',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        headers['Authorization'] = Webhooks.getAccessToken(self)

        url = 'http://117.132.184.21:8000/cdispatching/fault/v1/alarmExport/getAlarmByCondition'

        data = {
            'meetingId':'4f18dc0908bb498db56e952e267e7c89',
            'pageNo':'1',
            'pageSize':'10',
            'startTime':'2021-11-22 15:20:01',
            'endTime':'2021-11-23 15:20:01',
            'provinceList':[],
            'cityList':[],
            'alarmMajor':[]
        }

        headers.update(eval(headers1.replace('\"','\'').replace('“','\'').replace('”','\'')))
        data.update(eval(data1.replace('\"','\'').replace('“','\'').replace('”','\'')))

        self.log.debug(repr(request))
        self.log.debug(repr(headers))
        self.log.debug(repr(data))

        response = requests.post(url, headers = headers, data = demjson.encode(data))

        return response.text
