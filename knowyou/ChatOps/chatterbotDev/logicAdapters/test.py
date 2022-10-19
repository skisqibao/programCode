import os 

cur_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

print(type(cur_dir))
print(cur_dir)

log_path = os.path.join(cur_dir, "chatterbot.log")
print(log_path)

cur_dir = os.path.abspath(__file__).rsplit("/", 1)[0]

#print(cur_dir)
#print(type(cur_dir))
log_path = os.path.join(cur_dir, "chatterbot.log")


