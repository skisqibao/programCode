import numpy as np
import pandas as pd

# 引入sklearn里的数据集
from sklearn.datasets import load_iris
# 切分数据集为训练集和测试集
from sklearn.model_selection import train_test_split
# 计算分类预测的准确率
from sklearn.metrics import accuracy_score


iris = load_iris()
print(iris)

df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
# print(df)
df['class'] = iris.target
# print(df)
df['class'] = df['class'].map({
    0: iris.target_names[0],
    1: iris.target_names[1],
    2: iris.target_names[2]
})
# print(df)
# print(df.describe())

x = iris.data
y = iris.target.reshape(-1, 1)
# print(x.shape, y.shape)

# stratify=y, 按照y等比例分布，以防切分的集合太极端
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=35, stratify=y)

# print(y_test)
# print(x_train.shape, x_test.shape, y_train.shape, y_test.shape)

'''
print(x_train)
print(x_test[0].reshape(1, -1))
print('*' * 8)
print(np.abs(x_train - x_test[0].reshape(1, -1)))
print(np.sum(np.abs(x_train - x_test[0].reshape(1, -1))))
print(np.sum(np.abs(x_train - x_test[0].reshape(1, -1)), axis=1))
'''


# distance function
# a is a metric, raw is n, column is m
# b is a vector, raw is 1, column is m
def l1_distance(a, b):
    return np.sum(np.abs(a - b), axis=1)


def l2_distance(a, b):
    return np.sqrt(np.sum((a - b) ** 2, axis=1))


# classifier
class kNN(object):
    def __init__(self, n_neighbors=1, dist_func=l1_distance):
        self.n_neighbors = n_neighbors
        self.dist_func = dist_func

    def fit(self, x, y):
        self.x_train = x
        self.y_train = y

    def predict(self, x):
        y_pred = np.zeros((x.shape[0], 1), dtype=self.y_train.dtype)

        for i, x_test in enumerate(x):
            # 1. compute distance
            distances = self.dist_func(self.x_train, x_test)

            # 2. sort by distance and get the index
            nn_index = np.argsort(distances)

            # 3. select the latest points and save them classes
            nn_y = self.y_train[nn_index[:self.n_neighbors]].ravel()

            # 4. count the most frequent class and give this class to predict yi
            y_pred[i] = np.argmax(np.bincount(nn_y))

        return y_pred


knn = kNN(n_neighbors=3)

knn.fit(x_train, y_train)

pred_y = knn.predict(x_test)

accuracy = accuracy_score(y_test, pred_y)

print(accuracy)

knn = kNN(n_neighbors=3)

knn.fit(x_train, y_train)

result_list = []
for p in [1, 2]:
    knn.dist_func = l1_distance if p == 1 else l2_distance
    for k in range(1, 10, 2):
        knn.n_neighbors = k
        pred_y = knn.predict(x_test)
        accuracy = accuracy_score(y_test, pred_y)
        result_list.append([k, 'l1_distance' if p == 1 else 'l2_distance', accuracy])

df = pd.DataFrame(result_list, columns=['k', 'distance', 'accuracy'])
print(df)
