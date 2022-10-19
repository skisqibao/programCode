import logging

# This is a minimal configuration to get you started with the Text mode.
# If you want to connect Errbot to chat services, checkout
# the options in the more complete config-template.py from here:
# https://raw.githubusercontent.com/errbotio/errbot/master/errbot/config-template.py

BACKEND = 'Text'  # Errbot will start in text mode (console only mode) and will answer commands from there.
#BACKEND = 'Slack'

BOT_DATA_DIR = r'/root/sk/20211124/errbot/data'
BOT_EXTRA_PLUGIN_DIR = r'/root/sk/20211124/errbot/plugins'

BOT_LOG_FILE = r'/root/sk/20211124/errbot/errbot.log'
BOT_LOG_LEVEL = logging.DEBUG

BOT_ADMINS = ('@skisqibao','@CHANGE_ME')  # !! Don't leave that to "@CHANGE_ME" if you connect your errbot to a chat system !!

BOT_IDENTITY = {
    'token': 'xoxb-2763205708050-2761041605462-TwSDfFDJH0jMZ02fC7wWIb8b',
}

BOT_ALT_PREFIXES = ('Err',)
BOT_ALT_PREFIX_SEPARATORS = (':', ',', ';')

BOT_PREFIX = ''
