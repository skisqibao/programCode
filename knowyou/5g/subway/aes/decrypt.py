# _*_coding:utf-8_*_

import os
dirname = os.path.split(os.path.realpath(__file__))[0]
filename = os.path.join(dirname, "text.txt_encrypted")
import base64
from Crypto.Cipher import AES

def add_to_16(text):
    while len(text) % 16 != 0:
        text += '\0'
    return (text)

key = 'kadsfasw21!)+&*'
key = add_to_16(key) 

print(key)

cipher = AES.new(key.encode('utf-8'), AES.MODE_ECB)
print(filename)
data = bytearray(os.path.getsize(filename))
with open(filename, 'rb') as f:
    f.readinto(data)
    f.close()

decryption_data = cipher.decrypt(data)

with open(filename + '_decryption', 'ba') as f:
    f.write(decryption_data)
    f.close()


