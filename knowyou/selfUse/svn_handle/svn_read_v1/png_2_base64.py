import base64

filename = r"D:\a-sk\win_download\paimeng4-1.gif"

f = open(filename, "rb")
res = f.read()
s = base64.b64encode(res)
print(s)
f.close()


import rsa
s = base64.b64encode(bytes('songk','utf-8'))
print(s)

# 生成公钥、私钥
(pubkey, privkey) = rsa.newkeys(512)
print("公钥:\n%s\n私钥:\n:%s" % (pubkey, privkey))
# 明文编码格式
content = 'songk'.encode("utf-8")
# 公钥加密
crypto = rsa.encrypt(content, pubkey)
print(crypto)