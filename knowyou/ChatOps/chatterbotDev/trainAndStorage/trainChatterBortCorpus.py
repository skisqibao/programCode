# -*- coding: utf-8 -*-

from utils.readConfig import *

from chatterbot import ChatBot
from chatterbot.trainers import ChatterBotCorpusTrainer

import logging
logging.basicConfig(level=logging.DEBUG)

rc = ReadConfig()
#print(rc.database_uri)

chatbot = ChatBot(
    'Centralized Scheduling',
    storage_adapter='chatterbot.storage.SQLStorageAdapter',
    database_uri=rc.pymysql_uri
)

trainer = ChatterBotCorpusTrainer(chatbot)
trainer.train('chatterbot.corpus.chinese')

