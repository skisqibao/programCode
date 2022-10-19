#!/usr/bin/env python

str0="{'Connection': 'keep-alive'}"

print (str0)
cla=str(type(str0))
print (cla)

str1 = eval(str0)
print(str1)
cla=str(type(str1))
print(cla)

dict1={'content-type':'application/json;charset=UTF-8'}
print(dict1)
cla=str(type(dict1))
print(cla)

dict1.update(str1)
print(dict1)
cla=str(type(dict1))
print(cla)
