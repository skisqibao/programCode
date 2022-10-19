import rsa


pub, pri = rsa.newkeys(2048)    # 生成公钥、私钥
 
pri = pri.save_pkcs1()  # 保存为 .pem 格式
with open("pri.pem", "wb") as x:  # 保存私钥
    x.write(pri)
pub = pub.save_pkcs1()  # 保存为 .pem 格式
with open("pub.pem", "wb") as x:  # 保存公钥
    x.write(pub)


y = b"kadsfasw21!)+&*"

with open("pri.pem", "rb") as x:
    e = x.read()
    e = rsa.PrivateKey.load_pkcs1(e)    # load 私钥
with open("pub.pem", "rb") as x:
    f = x.read()
    f = rsa.PublicKey.load_pkcs1(f)     # load 公钥

print("before encrypt AES-key: ", y)

encrypt_key = rsa.encrypt(y, f)

print("use private key to encrypt AES-key: ", encrypt_key)
with open("encrypt_aes_key.txt", "wb") as x:
    x.write(encrypt_key)

decrypt_key = rsa.decrypt(encrypt_key, e)

print("use public key to decrypt AES-key: ", decrypt_key)
print(type(decrypt_key))
print(str(decrypt_key,encoding="utf-8"))
