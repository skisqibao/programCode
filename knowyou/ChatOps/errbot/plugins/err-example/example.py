from errbot import BotPlugin, botcmd
import json
import requests

class Example(BotPlugin):
    """
    This is a very basic plugin to try out your new installation and get you started.
    Feel free to tweak me to experiment with Errbot.
    You can find me in your init directory in the subdirectory plugins.
    """

    @botcmd  # flags a command
    def tryme(self, msg, args):  # a command callable with !tryme
        """
        Execute to check if Errbot responds to command.
        Feel free to tweak me to experiment with Errbot.
        You can find me in your init directory in the subdirectory plugins.
        """
        return "It *works* !"  # This string format is markdown.



    @botcmd
    def 甘雨(self, msg, args):
        return "这是你**老婆**！"


    @botcmd
    def getOwnerProvince(self, msg, args):
        headers = {'content-type':'application/json'}
        url = 'http://117.132.184.21:8000/cdispatching/monitor/v1/cut/baseinfo/getOwnerProvince'

        response = requests.get(url)

        data = response.json()
        body = data['body']

        id = body['id']
        pName = body['provinceName']

        reply = "编号：" + str(id) + '\n' + '省份：' + pName
        return reply
