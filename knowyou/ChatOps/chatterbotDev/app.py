# -*- coding: utf-8 -*-
from utils.basicUtil import getResultTextFormat
from utils.readConfig import *
from utils.commonLogger import *

from gevent import pywsgi
from flask import Flask, render_template, request

from chatterbot import ChatBot

app = Flask(__name__)

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


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/get")
def get_bot_response():
    userText = request.args.get('msg')
    logging.info('获取对话内容：' + userText)
    return str(chatbot.get_response(userText))


@app.route("/centralizedScheduling/<parameter>")
def get_centralized_scheduling(parameter):
    logging.info('获取对话内容：' + parameter)
    res = str(chatbot.get_response(parameter))
    return res if 'options' in res else getResultTextFormat(res)


if __name__ == "__main__":
    app.debug = True
    app.run('0.0.0.0', 5000)
    # server = pywsgi.WSGIServer(('0.0.0.0', 5000), app)
    # server.serve_forever()
