from chatterbot import ChatBot

cb = ChatBot(
    "test",
    logic_adapters=[
        {
            'import_path': 'queryAlert.QueryAlarmAdapter'
        }, {
            'import_path': 'logicAdapters.queryCutJoins.QuerySeriousHiddenDanger'
        }
    ]
)

if __name__ == '__main__':
    while True:
        request = input('You: ')
        response = cb.get_response(request)
        print('Bot: ', response)
