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


# 加密内容需要可以被16整除，所以进行空格拼接
def pad(text):
    while len(text) % 16 != 0:
        text += b' '
    return text


key = 'skisqibaokadsfasw21!)+&*'
key = add_to_16(key)

cipher = AES.new(key.encode('utf-8'), AES.MODE_ECB)

encrypted_data = cipher.encrypt(pad('songk'.encode()))

print(encrypted_data)

decrypted_data = cipher.decrypt(b'\x84]50=jS\x0b[,S#\xebwc\xf5')

print(decrypted_data.decode().strip())

import rsa

(public, private) = rsa.newkeys(512)
print(public)
print(private)

content = key.encode()

crypto_key = rsa.encrypt(content, public)
print(crypto_key)

origin_key = rsa.decrypt(crypto_key, private)
print(origin_key.decode().strip())

