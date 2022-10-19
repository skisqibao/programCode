import numpy as np
import pandas as pd

docA = 'The cat sat on my bed'
docB = 'The dog sat on my knees'

bowA = docA.split(' ')
bowB = docB.split(' ')

# 构建词库
wordSet = set(bowA).union(set(bowB))

# 进行词数统计
wordDcitA = dict.fromkeys(wordSet, 0)
wordDcitB = dict.fromkeys(wordSet, 0)

# 遍历文档统计词数
for word in bowA:
    wordDcitA[word] += 1
for word in bowB:
    wordDcitB[word] += 1

df = pd.DataFrame([wordDcitA, wordDcitB])


def computeTF(wordDict, bow):
    tfDict = {}
    nbow = len(bow)

    for word, count in wordDict.items():
        tfDict[word] = count / nbow

    return tfDict


tfA = computeTF(wordDcitA, bowA)
tfB = computeTF(wordDcitB, bowB)
print(df)
print(tfA)
print(tfB)


def computeIDF(wordDictList):
    idfDict = dict.fromkeys(wordDictList[0], 0)
    ndoc = len(wordDictList)

    import math

    for wordDict in wordDictList:
        # 遍历字典中的每个词汇，统计Ni
        for word, count in wordDict.items():
            if count > 0:
                idfDict[word] += 1

    for word, Ni in idfDict.items():
        idfDict[word] = math.log10((ndoc + 1) / (Ni + 1))

    return idfDict


idfs = computeIDF([wordDcitA, wordDcitB])
print(idfs)


# 计算tf-idf
def computeTFIDF(tf, idfs):
    tfidf = {}
    for word, tfval in tf.items():
        tfidf[word] = tfval * idfs[word]

    return tfidf


tfidfA = computeTFIDF(tfA, idfs)
tfidfB = computeTFIDF(tfB, idfs)
print(tfidfA)
print(tfidfB)
print(pd.DataFrame([tfidfA, tfidfB]))
