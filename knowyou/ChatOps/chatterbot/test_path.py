import os

cur_dir = os.path.abspath(__file__).rsplit("/", 1)[0]
print(cur_dir)

log_path = os.path.join(cur_dir, "chatterbot.log")
print(log_path)


print( os.path.abspath(os.path.dirname(os.path.dirname(__file__))) )
print( os.path.abspath(os.path.dirname(os.getcwd())) )
print( os.path.abspath(os.path.join(os.getcwd(), "..")) )
