# _*_coding:utf-8_*_

import os
dirname = os.path.split(os.path.realpath(__file__))[0]
filename = os.path.join(dirname, "text.txt")
import base64
from Crypto.Cipher import AES

def add_to_16(text):
    while len(text) % 16 != 0:
        text += '\0'
    return (text)

#加密内容需要可以被16整除，所以进行空格拼接
def pad(text):
    while len(text) % 16 != 0:
        text += b' '
    return text

key = 'kadsfasw21!)+&*'
key = add_to_16(key) 

cipher = AES.new(key.encode('utf-8'), AES.MODE_ECB)
print(filename)
data = bytearray(os.path.getsize(filename))
with open(filename, 'rb') as f:
    f.readinto(data)
    f.close()

encrypted_data = cipher.encrypt(pad(data))

with open(filename + '_encrypted', 'ba') as f:
    f.write(encrypted_data)
    f.close()


