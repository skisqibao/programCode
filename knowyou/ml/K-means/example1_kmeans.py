import numpy as np
import matplotlib.pyplot as plt

# 从sklearn中直接生成聚类数据
from sklearn.datasets._samples_generator import make_blobs

x, y = make_blobs(n_samples=100, centers=6, random_state=1234, cluster_std=0.7)

# print(x)
# print(y)
# plt.figure(figsize=(6, 6))
# plt.scatter(x[:, 0], x[:, 1], c=y)
# plt.show()

# 引入scipy中的距离函数，默认欧式距离
from scipy.spatial.distance import cdist


class K_Means(object):
    def __init__(self, n_clusters=6, max_iter=300, centroids=[]):
        self.n_clusters = n_clusters
        self.max_iter = max_iter
        self.centroids = np.array(centroids, dtype=np.float)

    def fit(self, data):
        if (self.centroids.shape == (0,)):
            # 从data中随机生成clusters个整数，作为质心
            self.centroids = data[np.random.randint(0, data.shape[0], self.n_clusters), :]

        for i in range(self.max_iter):
            # 1.计算距离矩阵, 传入两个矩阵直接计算得到所有点到每个质心的距离
            distances = cdist(data, self.centroids)

            # 2.对距离按照由近到远排序，选取最近的质心点的类别，作为当前点的分类
            c_index = np.argmin(distances, axis=1)

            # 3.对每一类数据进行均值计算，更新质心点坐标
            for i in range(self.n_clusters):
                # 排除掉没有出现在c_index中的类别
                if i in c_index:
                    # 选出所有类别是i的点，取data里面坐标的均值，更新第i个质心
                    self.centroids[i] = np.mean(data[c_index == i], axis=0)

    def predict(self, samples):
        # 计算距离矩阵，选取距离最近的质心类别
        distances = cdist(samples, self.centroids)
        c_index = np.argmin(distances, axis=1)

        return c_index


# dist = np.array([[1, 2, 3, 4, 5],
#                  [2, 1, 3, 4, 5],
#                  [4, 2, 3, 1, 5],
#                  [5, 3, 4, 2, 1],
#                  [5, 1, 3, 4, 2]
#                  ])
# c_ind = np.argmin(dist, axis=1)
# print(c_ind)
# x_new = x[0:5]
# print(x_new)
# 分组
# print(x_new[c_ind == 1])
# 求每组质心
# cen = np.mean(x_new[c_ind == 1], axis=0)
# print(cen)

def plotKMeans(x, y, centroids, subplot, title):
    plt.subplot(subplot)
    plt.scatter(x[:, 0], x[:, 1], c='r')
    plt.scatter(centroids[:, 0], centroids[:, 1], c=np.array(range(6)), s=100)
    plt.title(title)


kmeans = K_Means(max_iter=300, centroids=np.array([[2, 1], [2, 2], [2, 3], [2, 4], [2, 5], [2, 6]]))
plt.figure(figsize=(16, 6))
# subplot 121代表一共一行两列的子图，当前是第一个
plotKMeans(x, y, kmeans.centroids, 121, 'initial State')

kmeans.fit(x)

plotKMeans(x, y, kmeans.centroids, 122, 'Final State')

x_new = np.array([[0, 0], [10, 7]])
y_pred = kmeans.predict(x_new)
print(y_pred)

plt.scatter(x_new[:, 0], x_new[:, 1], s=110, c='pink')
plt.show()
