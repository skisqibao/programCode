from chatterbot import ChatBot
from chatterbot.trainers import ChatterBotCorpusTrainer

# 初始化机器人
chatbot = ChatBot("deepThought")
chatbot.set_trainer(ChatterBotCorpusTrainer)

# 这里先使用该库现成的中文语料库训练
chatbot.train("chatterbot.corpus.chinese")

from chatterbot.trainers import ListTrainer

# 使用ListTrainer进行自定义训练，输入内容为一个列表
chatbot.set_trainer(ListTrainer)
chatbot.train([
    "讲个笑话",
    "一天和同学出去吃饭，买单的时候想跟服务员开下玩笑。“哎呀，今天没带钱出来埃”“你可以刷卡。”“可是我也没带卡出来的埃”“那你可以刷碗“",
])

from wxpy import *

# 初始化机器人，这里会生成一张二维码，用微信扫码继续登陆
bot = Bot()
# 获取好友列表，这里随意使用一个好友进行测试
bot.friends()
friend = bot.friends()[0]
# 向好友发送消息
friend.send('hi')
# 使用机器人进行自动回复
@bot.register(friend)
def reply_my_friend(msg):
    return chatbot.get_response(msg.text).text

