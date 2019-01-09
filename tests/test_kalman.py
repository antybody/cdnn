#-*-coding:utf-8-*-

import numpy as np
import matplotlib.pyplot as plt

'''
 point a
'''
#satae mode

# a = 1/2
a = 1
n_iter = 100
sz = (n_iter,)


# 真实值
X = np.zeros(sz)
# 测量值
z = np.zeros(sz)
# 估计值
state_kalman = np.zeros(sz)
# 预测值
state_pre = np.zeros(sz)
# 方差
P = np.zeros(sz)

Pminus = np.zeros(sz)

# 增益
K = np.zeros(sz)

# 噪声
k = 1
# R = 2
R = 0.04
# Q = np.power(0.5,k) # 过程噪声
Q = 0.09

state_kalman[0] = 0    # 温度初始值

P[0] = 1.0               # 初始估计方差
X[0] = 0

# 随机给定测量值，加入噪声

# 模拟真实值 和观测值
for k in range(1,n_iter):
    X[k] = a * X[k-1] + R
    z[k] = X[k] + Q

print(X)

# # 更新
# for k in range(1,n_iter):
#
#     state_pre[k] = a*state_kalman[k-1]  # 预测值
#     # print(state_pre)
#     Pminus[k] = np.power(a,2)*P[k-1]+R
#
#     K[k] = Pminus[k] / (Pminus[k] + Q) # 增益
#
#     state_kalman[k] = state_pre[k] + K[k] * (z[k] - state_pre[k]) # 每次的权重不同
#
#     P[k]  = (1 -K[k]) * Pminus[k]
#
#
# #
# #
# plt.figure()
# plt.plot(z,color='r',label = 'noisy measurements')
# plt.plot(state_kalman,color='b',label = 'estimate')
# plt.plot(X,color = 'g',label = 'true value')
#
# plt.legend()
# plt.title('Estimate vs. iteration step',fontweight = 'bold')
# plt.xlabel('Iteration')
# plt.ylabel('Voltage')
#
#
# plt.show()
