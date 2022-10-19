from chatterbot.logic import LogicAdapter
from chatterbot.conversation import Statement

from utils.basicUtil import *
from utils.performanceUtil import *
from utils.commonLogger import *


class QueryPerformanceAdapter(LogicAdapter):
    def __init__(self, chatbot, **kwargs):
        super().__init__(chatbot, **kwargs)
        self.labelList = getLabelList()
        self.typeSet = getTypeSet()

    def can_process(self, statement):
        for label in self.labelList:
            if label in statement.text:
                return True

        for typ in self.typeSet:
            if typ in statement.text:
                return True

        if '性能' in statement.text or '网元' in statement.text:
            return True
        else:
            return False

    def process(self, statement, additional_response_selection_parameters=None):

        logging.info('性能查询入口')
        infos = statement.text.split('#')
        # 2022-03-14#HSS#HSS01#注册用户数
        if len(infos) != 4:
            logging.info('性能查询，格式有误提示')
            response_statement = Statement(
                text=getResultTextFormat('查询网元性能请输入正确格式：时间#类型#网元#指标,例如：2022-03-14#HSS#HSS01#注册用户数'))
        else:
            logging.info('性能查询，格式正常，请求为：' + str(infos))
            res = queryPerformance(infos[0], infos[1], infos[2], infos[3])
            response_statement = Statement(text=getResultTextFormat(res))

        response_statement.confidence = 1

        return response_statement
