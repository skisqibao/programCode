# simple linear regression（least square）
import numpy as np
import matplotlib.pyplot as plt

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

print(x)
print(y)

plt.scatter(x, y)
plt.show()


# lost function
def compute_cost(w, b, points):
    total_cost = 0
    M = len(points)

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]
        total_cost += (y - w * x - b) ** 2

    return total_cost / M


# average function
def avg(data):
    sum = 0
    num = len(data)
    for i in range(num):
        sum += data[i]

    return sum / num


# core algorithm to fit function
def fit(points):
    M = len(points)
    x_bar = avg(points[:, 0])

    w_numerator = 0
    w_denominator = 0
    b_delta = 0

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]

        w_numerator += y * (x - x_bar)
        w_denominator += x ** 2
    w = w_numerator / (w_denominator - M * (x_bar ** 2))

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]
        b_delta += (y - w * x)
    b = b_delta / M
    return w, b


w, b = fit(points)
print('w is ', w)
print('b is ', b)
print('cost is ', compute_cost(w, b, points))

plt.scatter(x, y)

pred_y = w * x + b
plt.plot(x, pred_y, c='r')
plt.show()
