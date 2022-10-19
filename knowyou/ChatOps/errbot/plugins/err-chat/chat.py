import json
import requests
import re
from errbot import BotPlugin, botcmd, arg_botcmd, re_botcmd

class Chat(BotPlugin):

    # 自动切分参数
    @botcmd(split_args_with=None)  
    def 华飞(self, msg, args): 
        reply = ''

        for arg in args:
            if arg == '翀':
                reply = reply + arg + "：来自管理员的诱惑\n"
            if arg == '车干':
                reply = reply + arg + "：来自卫星的困惑\n"
            if arg == '屑':
                reply = reply + arg + "：卧龙凤雏的迷惑\n"
            if arg == '鹤翔':
                reply = reply + arg + "：酒桌饭局的魅惑\n"

        return reply  


    # 子指令用法
    @botcmd
    def 璃月_甘雨(self, msg, args):
        return "这是你**老婆**！"
    @botcmd
    def 璃月_申鹤(self, msg, args):
        return "这是你**二老婆**，甘雨第大，其他往后稍稍！"

  
    # 指定参数
    @arg_botcmd('name', type=str)
    @arg_botcmd('--sex', dest='sex', type=str)
    @arg_botcmd('--age', dest='age', type=int, default=18)
    @arg_botcmd('--favorite', dest='favorite', type=str)
    def 谁是(self, msg, name=None, sex=None, age=None, favorite=None):
        headers = {'content-type':'application/json'}
        url = 'http://cn.bing.com'
        arg_list = {}

#        response = requests.get(url)

#        data = response.json()
#        body = data['body']


        reply = "关于" + name + ', 性别' + sex  + '我猜TA大概率' + str(age) + '岁\n' + '可能很喜欢加班，建议你先bing查查再来问我。'
        return reply


    # 带前缀的正则匹配
    @re_botcmd(pattern=r"^(([我俺咱])想看)?魔禁4或超炮4(\!)?$")
    def pray_JCSTAFF(self, msg, match):
        yield "天国的魔禁超炮系列，我也想看，现在就想看┭┮﹏┭┮，{}".format(msg.frm)
        yield "/me 等不下去了，得多少年。想想就哭了"

    # 不带前缀的正则匹配
    @re_botcmd(pattern=r"((魔禁)|[男女])?角色", prefixed=False, flags=re.IGNORECASE)
    def pray_JCSTAFF_Character(self, msg, match):
        reply = "教主，食蜂操祈，小黄书，炮姐..."
        return reply

