# simple linear regression（gradient descent）
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


# print(x)
# print(y)

# plt.scatter(x, y)
# plt.show()


# lost function
def compute_cost(w, b, points):
    total_cost = 0
    M = len(points)

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]
        total_cost += (y - w * x - b) ** 2

    return total_cost / M


# defined hyper-parameter of model
alpha = 0.1
initial_w = 0
initial_b = 0
num_iter = 30


def step_grad_desc(current_w, current_b, alpha, points):
    sum_grad_w = 0
    sum_grad_b = 0
    M = len(points)

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]
        sum_grad_w += (current_w * x + current_b - y) * x
        sum_grad_b += current_w * x + current_b - y

    # Partial derivative
    grad_w = 2 / M * sum_grad_w
    grad_b = 2 / M * sum_grad_b

    # Iterate theta
    updated_w = current_w - alpha * grad_w
    updated_b = current_b - alpha * grad_b

    return updated_w, updated_b


# core gradient descent function
def grad_desc(points, initial_w, initial_b, alpha, num_iter):
    w = initial_w
    b = initial_b

    # save all lost value
    cost_list = []

    for i in range(num_iter):
        cost_list.append(compute_cost(w, b, points))
        w, b = step_grad_desc(w, b, alpha, points)

    return [w, b, cost_list]


w, b, cost_list = grad_desc(points, initial_w, initial_b, alpha, num_iter)

print('w is ', w)
print('b is ', b)
print('cost is ', compute_cost(w, b, points))

plt.plot(cost_list)
plt.show()

plt.scatter(x, y)
pred_y = w * x + b
plt.plot(x, pred_y, c='r')
plt.show()