# _*_coding:utf-8_*_

import os
dirname = os.path.split(os.path.realpath(__file__))[0]
from Crypto.Cipher import AES
import rsa

def get_aes_key():
    # load private key
    with open("pri.pem", "rb") as x:
        e = x.read()
        e = rsa.PrivateKey.load_pkcs1(e)   
        x.close()

    # load aes-key encrypted with private key 
    with open(os.path.join(dirname, "encrypt_aes_key.txt"), "rb") as x:
        encrypt_key = x.read()
        x.close()

    decrypt_key = rsa.decrypt(encrypt_key, e)
    return str(decrypt_key, encoding="utf-8")


def add_to_32(text):
    while len(text) % 32 != 0:
        text += '\0'
    return (text)

key = add_to_32(get_aes_key()) 

cipher = AES.new(key.encode('utf-8'), AES.MODE_ECB)

input_path = os.path.join(dirname, "encrypt_subway_data")
output_path = os.path.join(dirname, "decrypt_subway_data")
all_file_list = os.listdir(input_path)

for filename in all_file_list:
    abs_file_path = os.path.join(input_path, filename)
    data = bytearray(os.path.getsize(abs_file_path))
    with open(abs_file_path, 'rb') as x:
        x.readinto(data)
        x.close()
    
    decryption_data = cipher.decrypt(data)
    
    with open(os.path.join(output_path, filename) + '_decryption.csv', 'ba') as x:
        x.write(decryption_data)
        x.close()


