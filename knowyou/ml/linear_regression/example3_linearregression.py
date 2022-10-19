# simple linear regression
import numpy as np
import matplotlib.pyplot as plt

from sklearn.linear_model import LinearRegression

# points = np.genfromtxt('data.txt', delimiter=',')
data = [
    [0.067732, 3.176513], [0.427810, 3.816464], [0.995731, 4.550095], [0.738336, 4.256571], [0.981083, 4.560815],
    [0.526171, 3.929515], [0.378887, 3.526170], [0.033859, 3.156393], [0.132791, 3.110301], [0.138306, 3.149813],
    [0.247809, 3.476346], [0.648270, 4.119688], [0.731209, 4.282233], [0.236833, 3.486582], [0.969788, 4.655492],
    [0.607492, 3.965162], [0.358622, 3.514900], [0.147846, 3.125947], [0.637820, 4.094115], [0.230372, 3.476039],
    [0.070237, 3.210610], [0.067154, 3.190612], [0.925577, 4.631504], [0.717733, 4.295890], [0.015371, 3.085028],
    [0.335070, 3.448080], [0.040486, 3.167440], [0.212575, 3.364266], [0.617218, 3.993482], [0.541196, 3.891471]
]

points = np.array(data)

# extract X and Y from points
x = points[:, 0]
y = points[:, 1]

# lost function
def compute_cost(w, b, points):
    total_cost = 0
    M = len(points)

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]
        total_cost += (y - w * x - b) ** 2

    return total_cost / M

# transfer 1D to 2D
# -1代表行数不限，一共1列
x_new = x.reshape(-1, 1)
y_new = y.reshape(-1, 1)
print(x)
print(x_new)

# call model
lr = LinearRegression()
lr.fit(x_new, y_new)

w = lr.coef_
b = lr.intercept_

print('w is ', w)
print('b is ', b)
print('cost is ', compute_cost(w, b, points))

plt.scatter(x, y)
pred_y = w[0,0] * x + b[0]
plt.plot(x, pred_y, c='r')
plt.show()