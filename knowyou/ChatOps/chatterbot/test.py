# -*- coding: utf-8 -*-
from chatterbot import ChatBot
from chatterbot.trainers import ListTrainer
from chatterbot.trainers import ChatterBotCorpusTrainer

# Create a new chat bot named Charlie
chatbot = ChatBot(
    'Test1'
)

trainer = ListTrainer(chatbot)
 
trainer.train([
    "每年的职业棒球赛",
    "金手套。",
    "什么是篮球",
    "一个身高占很大影响因素的游戏，可惜我不够高。",
    "什么足球",
    "我天生没有运动基因。",
    "什么是棒球",
    "我不知道。能吃吗？",
    "什么是足球",
    "一场比赛由两名11名球员在场上进球并在球场两端各打一球; 主要通过踢或使用身体的任何部分（除了手和手臂）来移动球。。",
    "我爱棒球",
    "我不喜欢做太多运动。",
    "我踢足球",
    "你必须跑得很快才可以。",
    "我打板球",
    "你喜欢打哪个位置？",
    "什么是板球",
    "板球是一个用球拍与球的比赛，在板球场上由11名球员组成的球队之间进行比赛，球场的中央是一个长22码的长方形球场，",
    "两端分别设有一个检票口（一组三个木桩）。",
    "我打排球",
    "是否占用了大量的时间？",
    "你踢足球吗",
    "我不知道怎么玩。",
    "你打篮球",
    "不,我没有协调。",
    "你知道男篮",
    "什么是篮球吗？",
    "你想打篮球",
    "我只是个网瘾少女。",
    "喜欢篮球",
    "我也是。",
    "你是一个足球",
    "我才不是。",
    "谁是最伟大的棒球运动员",
    "乔治·赫尔曼·露丝。相当贝贝。",
    "谁是最好的足球运动员",
    "马拉多纳是伟大的。 Sinsemillia甚至更好。",
    "告诉我关于棒球",
    "什么是棒球"
])
 
# Get a response to the input text 'How are you?'

#trainer = ChatterBotCorpusTrainer(chatbot)
#trainer.train('chatterbot.corpus.chinese')

while True:
    request = input('You: ')
    response = chatbot.get_response(request) 
    print('Bot: ', response)
    
