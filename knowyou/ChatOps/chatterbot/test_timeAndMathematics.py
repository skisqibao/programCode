from chatterbot import ChatBot


bot = ChatBot(
    'Math & Time Bot',
    logic_adapters=[
        'chatterbot.logic.MathematicalEvaluation',
        'chatterbot.logic.TimeLogicAdapter'
    ]
)

# Print an example of getting one math based response
response = bot.get_response('4 + 9?')
print(response)

# Print an example of getting one time based response
response = bot.get_response('现在几点了?')
print(response)
