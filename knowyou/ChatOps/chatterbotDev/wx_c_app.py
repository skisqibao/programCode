# -*- coding: utf-8 -*-
from utils.basicUtil import getResultTextFormat
from utils.readConfig import *
from utils.commonLogger import *

from chatterbot import ChatBot

rc = ReadConfig()

# Set up your bot
chatbot = ChatBot(
    "Centralized Scheduling",
    storage_adapter="chatterbot.storage.SQLStorageAdapter",
    database_uri=rc.pymysql_uri,
    logic_adapters=[
        {
            'import_path': 'logicAdapters.queryAlert.QueryAlarmAdapter'
        }, {
            'import_path': 'logicAdapters.queryAlert.QueryComplaintAdapter'
        }, {
            'import_path': 'logicAdapters.queryCutJoins.QueryCutJoinsAdapter'
        }, {
            'import_path': 'logicAdapters.queryCutJoins.QuerySeriousHiddenDanger'
        }, {
            'import_path': 'logicAdapters.queryIndemnification.QueryRankOfSafeDays'
        }, {
            'import_path': 'logicAdapters.querySevenDaysWeather.QuerySevenDaysWeather'
        }, {
            'import_path': 'logicAdapters.queryPerformance.QueryPerformanceAdapter'
        },

        'chatterbot.logic.BestMatch',
        'chatterbot.logic.MathematicalEvaluation'
    ]
)

import msgDB
msgDB.initDB()

while True:
    res = msgDB.listen_wxMsg()  # 监听一次是否有新消息
    if res == False:  # 无新消息产生则开始下一轮监听
        continue

    print(res[0], res[3])
    response = str(chatbot.get_response(res[3]))
    print(response)

    msgDB.send_wxMsg(res[0], response)  # res[0]是发送消息的人的id
    msgDB.delMsg()
