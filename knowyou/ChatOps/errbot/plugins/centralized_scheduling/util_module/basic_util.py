import json
import requests
import os

dirname = os.path.split(os.path.realpath(__file__))[0]

def getAccessToken(authorization_ip, authorization_port):
    headers = {'Authorization': 'Basic dGVuYW50X2ppdHVhbjpGbHp4M3FjMTIzQA=='}

    url = 'http://' + authorization_ip + ':' + authorization_port + '/cdispatching/user/oauth/token?grant_type=client_credentials'

    response = requests.post(url, headers=headers).json()

    access_token = response['access_token']
    token_type = response['token_type']

    authorization = token_type + " " +  access_token

    return authorization

def getResultTextFormat(content):
    formattedContent = '{"options":[],"tailTip":"","title":"' + content + '"}'
    return formattedContent

def getResultTableFormat(content):
    pass


def getProvinceNameDictionary():
    provinceDict = {}
    fileName = dirname + '/dictionary/province_code.bcp'
    with open(fileName) as f:
        for line in f:
            (code, name) = line.split('\t')
            provinceDict[code] = name.strip()
    
    return provinceDict

if __name__ == '__main__':

    #print(getResultTextFormat("昨日告警总数123"))
    proD = getProvinceNameDictionary()
    print(proD.get("BJ"))
