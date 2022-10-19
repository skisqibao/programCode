# _*_coding:utf-8_*_

import os
dirname = os.path.split(os.path.realpath(__file__))[0]
from Crypto.Cipher import AES
import rsa

def get_aes_key():
    # load private key
    with open(os.path.join(dirname, "pri.pem"), "rb") as x:
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

def get_metro_dataframe(filename):

    key = add_to_32(get_aes_key()) 

    cipher = AES.new(key.encode('utf-8'), AES.MODE_ECB)

    data = bytearray(os.path.getsize(filename))
    with open(filename, 'rb') as x:
        x.readinto(data)
        x.close()
    
    from Crypto.Util.Padding import unpad
    #decryption_data = unpad(cipher.decrypt(data), 16)
    decryption_data = cipher.decrypt(data)
    #a = decryption_data.decode('gbk','ignore')
    a = decryption_data.decode('utf-8-sig','ignore')
    a = a.strip() 
    print(a) 
    import io 
    import pandas as pd

    df = pd.read_csv(io.StringIO(a), sep=',')
    return df

if __name__ == '__main__':
    df0 = get_metro_dataframe("/root/sk/5gdata/subway/aes_rsa_metro/encrypt_subway_data/shanghai-metro-4")
    df1 = get_metro_dataframe("/root/sk/5gdata/subway/aes_rsa_metro/encrypt_subway_data/beijin-metro-19")
    df2 = get_metro_dataframe("/root/sk/5gdata/subway/aes_rsa_metro/encrypt_subway_data/beijin-metro-2-jst")

    print(df0)
    print(df1)
    print(df2)
