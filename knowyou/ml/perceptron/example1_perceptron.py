from typing import List
import random
from sklearn.datasets import load_iris


class Perceptron:
    def __init__(self, eta: float = 1.0, max_iter: int = 1000):
        # 初始化学习率和最大迭代次数
        self.eta = eta
        self.max_iter = max_iter

        random.seed(1)

    def fit(self, x: List[List[float]], y: List[int]) -> None:
        if len(x) <= 0:
            return

        # 选取初始值
        self._w = [random.randint(-10, 10) for _ in range(len(x[0]) + 1)]

        times = 0
        while times < self.max_iter:
            times += 1
            errors = 0

            for xi, yi in zip(x, y):
                # 在训练集中选取数据
                y_predict = sum([self._w[i + 1] * xi[i] for i in range(len(xi))]) + self._w[0]
                # 如果是误分类点
                if yi * y_predict < 0:
                    errors += 1

                    # 进行参数更新
                    for i in range(len(xi)):
                        self._w[i + 1] += self.eta * yi * xi[i]
                    self._w[0] += self.eta * yi

                print(f'times: {times}, xi: {xi}, yi: {yi}, y_predict: {y_predict}, _w: {self._w}')

            if errors == 0:
                break

    def predict(self, xi: List[float]) -> int:
        return 1 if sum([self._w[i + 1] * xi[i] for i in range(len(xi))]) + self._w[0] >= 0 else -1


if __name__ == '__main__':
    p = Perceptron(eta=1.0, max_iter=100)

    # x = [[3, 3], [4, 3], [1, 1]]
    # y = [1, 1, -1]
    # p.fit(x, y)

    iris = load_iris()
    print(iris)
    # print(iris['data'][0:49])
    # print(iris['target'][0:49])

    # print(iris['data'][50:99])
    # print(iris['target'][50:99])

    p.fit(iris['data'][0:99], [-1 if i == 0 else i for i in iris['target'][0:99]])

