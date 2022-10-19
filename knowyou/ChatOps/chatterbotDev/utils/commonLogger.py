import logging
import os

cur_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
log_path = os.path.join(cur_dir, "chatterbot.log")
# encoding='utf-8'
logging.basicConfig(filename=log_path, level=logging.DEBUG,
                    filemode='a', format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
