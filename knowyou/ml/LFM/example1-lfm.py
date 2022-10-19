import numpy as np
import pandas as pd

'''
@:param
R:M*N的评分矩阵
K:隐特征向量维度
max_iter:最大迭代次数
alpha:步长
lambda:正则化系数

@:return
分解之后的P，Q
P:初始化用户特征矩阵M*K
Q:初始化物品特征矩阵N*K
'''

R = np.array([[4, 0, 2, 0, 1],
              [0, 2, 3, 0, 0],
              [1, 0, 2, 4, 0],
              [5, 0, 0, 3, 1],
              [0, 0, 2, 5, 1],
              [0, 3, 2, 4, 1]])

# row number
print(R.shape[0])
print(len(R))
# column number
print(R.shape[1])
print(len(R[0]))

# 给定超参数
K = 5
max_iter = 5000
alpha = 0.0002
lamb = 0.004


def LFM_gradient_descent(R, K=2, max_iter=1000, alpha=0.0001, lamb=0.002):
    # 基本维度参数
    M = len(R)
    N = len(R[0])

    # PQ初始值，随机生成
    P = np.random.rand(M, K)
    Q = np.random.rand(N, K)
    Q = Q.T

    # 迭代
    for step in range(max_iter):
        # 对所有用户u，物品i做遍历，对应的特征向量Pu、Qi梯度下降
        for u in range(M):
            for i in range(N):
                # 对每一个大于0的评分，求出预测与真实值的误差
                if R[u][i] > 0:
                    eui = np.dot(P[u, :], Q[:, i]) - R[u][i]

                    # 代入公式，按照梯度下更新P和
                    for k in range(K):
                        P[u][k] = P[u][k] - alpha * (2 * eui * Q[k][i] + 2 * lamb * P[u][k])
                        Q[k][i] = Q[k][i] - alpha * (2 * eui * P[u][k] + 2 * lamb * Q[k][i])

        # u,i遍历完成，所有特征向量更新完成，得到P、Q,计算预测评分矩阵
        predR = np.dot(P, Q)

        # 计算当前损失函数
        cost = 0
        for u in range(M):
            for i in range(N):
                if R[u][i] > 0:
                    cost += (np.dot(P[u, :], Q[:, i]) - R[u][i]) ** 2
                    # 正则化项
                    for k in range(K):
                        cost += lamb * (P[u][k] ** 2 + Q[k][i] ** 2)
        if cost < 0.0001:
            break

    return P, Q, cost


P, Q, cost = LFM_gradient_descent(R, K, max_iter, alpha, lamb)
print(P)
print(Q)
print(cost)

predR = P.dot(Q)
print(R)
print(predR)
