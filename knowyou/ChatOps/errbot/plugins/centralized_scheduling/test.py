import sys
import configparser

from util_module.basic_util import getAccessToken 

dirname = sys.path[0]
filename = dirname + "/config.ini"
#print(filename)

config = configparser.ConfigParser()
config.read(filename, encoding="utf-8")

authorization_ip = config["ip_port"]["authorization_ip"]
authorization_port = config["ip_port"]["authorization_port"]

request_ip='117.132.184.21'
request_port='8000'

#print(getAccessToken(authorization_ip, authorization_port))

strjson = '''
{
  "orders": [
    {
      "column": "cut_grade"
    }
  ],
  "cutProvince": "",
  "countTime": "2021-12-29",
  "cutGrades": [ 1 ],
  "cutStatus": "",
  "cutResult": "",
  "isOvertime": "",
  "isException": "",
  "pageSize": 200,
  "applicationId": 2
}
'''
#print(type(strjson))
#print(type(eval(strjson)))

strdict = '''
{
  'orders': [
    {
      'column': 'cut_grade'
    }
  ],
  'cutProvince': ',
  'countTime': '2021-12-29',
  'cutGrades': [ 1 ],
  'cutStatus': ',
  'cutResult': ',
  'isOvertime': ',
  'isException': ',
  'pageSize': 200,
  'applicationId': 2
}
'''
#print(type(strdict))

def test1():
    dataStr = '''
      {
        "orders": [
          {
            "column": "cut_grade"
          }
        ],
        "cutProvince": "",
        "countTime": "2021-12-29",
        "cutGrades": [
          1
        ],
        "cutStatus": "",
        "cutResult": "",
        "isOvertime": "",
        "isException": "",
        "pageSize": 200,
        "applicationId": 2
      }
    '''
    dataStr2 = '{"orders":[{"column":"cut_grade"}],"cutProvince":"","cutGrades":[1],"cutStatus":"","cutResult":"","isOvertime":"","isException":"","pageSize":200,"applicationId":2}'
    data = eval(dataStr2)
    print(type(data))

#test1()

data='''
{
    "body":{
        "list":[
            {
                "actualFinishTime":"",
                "actualStartTime":"",
                "cutCategory":"工程割接",
                "cutCreator":"",
                "cutDesc":"（A类操作）重庆移动D平面19700设备版本升级割接",
                "cutGrade":"A类",
                "cutId":"7637c2a2-92e3-41a8-a532-1e7e8d8d0c7e",
                "cutLevel":"二干",
                "cutNo":"重庆移动传输网计划于12月30日对OTN设备进行版本升级操作",
                "cutPlanFinishTime":"2021-12-30 05:00:00",
                "cutPlanStartTime":"2021-12-30 00:00:00",
                "cutProvince":"重庆",
                "cutRemark":"",
                "cutResult":"实施中",
                "cutResultDesc":"",
                "cutStatus":"工程预约",
                "cutType":"普通割接",
                "devAddr":"沙坪坝区",
                "existProblem":"否",
                "factory":"中兴",
                "impactBusTime":"",
                "impactDesc":"业务会有5到10分钟左右的中断，业务双平面保护，不影响最终业务",
                "impactEnterprise":"不影响",
                "impactProvince":"",
                "isException":"否",
                "isImpactBiz":"否",
                "isSimplify":"否",
                "ivrNoticeResult":"",
                "ivrNoticeStatus":"",
                "managerDept":"全业务支撑中心/全业务支撑中心传输网室",
                "managerName":"张楠",
                "managerPhone":"13983354695",
                "neDevAddr":"",
                "neName":"1902-两江新区人和枢纽楼18楼扩OTN,1928-黔江谭家湾楼新扩OTN,1905-北碚城南综合楼扩OTN,1904-沙坪坝西永综合楼A区OTN,1945-沙坪坝西永综合楼A区7楼OTN",
                "neType":"OTN",
                "operateType":"版本升级",
                "orderId":"f8486cbd-b784-49ce-9fda-53e24974afaa",
                "orderNo":"CQ-0001-20211227-006579",
                "orderSubject":"重庆移动传输网计划于12月30日对[OTN]设备进行[版本升级]操作",
                "smsNoticeResult":"",
                "smsNoticeStatus":"",
                "specialty":"传输网",
                "state":"",
                "taskProgress":"0.00%"
            },
            {
                "actualFinishTime":"",
                "actualStartTime":"",
                "cutCategory":"网络变更",
                "cutCreator":"",
                "cutDesc":"本次对内蒙古移动省干PTN LTE进行相关倒换测试，验证PTN网络及对接核心网，MDCN等保护运作状态",
                "cutGrade":"A类",
                "cutId":"8ceb318c-b6c3-470a-b91a-193ff173dd74",
                "cutLevel":"二干",
                "cutNo":"内蒙古移动传输网计划于12月30日对PTN设备进行倒换及应急演练操作",
                "cutPlanFinishTime":"2021-12-30 06:00:00",
                "cutPlanStartTime":"2021-12-30 00:00:00",
                "cutProvince":"内蒙古",
                "cutRemark":"",
                "cutResult":"实施中",
                "cutResultDesc":"",
                "cutStatus":"割接准备",
                "cutType":"普通割接",
                "devAddr":"呼和浩特市",
                "existProblem":"否",
                "factory":"华为",
                "impactBusTime":"",
                "impactDesc":"正常情况下不影响业务",
                "impactEnterprise":"不影响",
                "impactProvince":"",
                "isException":"否",
                "isImpactBiz":"否",
                "isSimplify":"否",
                "ivrNoticeResult":"",
                "ivrNoticeStatus":"",
                "managerDept":"网络管理中心/传输维护室",
                "managerName":"陈安宇",
                "managerPhone":"15049199140",
                "neDevAddr":"",
                "neName":"",
                "neType":"PTN",
                "operateType":"倒换及应急演练",
                "orderId":"ff9d593e-3479-4104-8cfe-6318c4885e6f",
                "orderNo":"NMG-0001-20211229-005670",
                "orderSubject":"内蒙古移动传输网计划于12月30日对[PTN]设备进行[倒换及应急演练]操作",
                "smsNoticeResult":"",
                "smsNoticeStatus":"",
                "specialty":"传输网",
                "state":"",
                "taskProgress":"0.00%"
            }
        ],
        "pageNum":1,
        "pageSize":200,
        "pages":1,
        "total":5
    },
    "code":1,
    "message":"操作成功"
}
'''
datadict = eval(data)
print(type(datadict))
datalist = datadict["body"]["list"]
print(type(datalist))

res = ""
for index in range(len(datalist)):
	onedict = datalist[index]
	print(type(onedict))
	res += str(index) + "." + onedict["cutNo"] + "\n"

print(res)
