#-*-coding:utf-8-*-
'''
 基于聚类和密度的离群点检测方法 陶晶
 该算法来源于 以上毕业论文
 思路是 对数据集对象进行 kmean聚类，找出 异常点的候选集群，然后进行lof 判断
 这里有个问题：1、数据需要有维度 2、异常的点都被聚类在一起
 参考：https://www.jianshu.com/p/2f8b370b4100
 https://blog.csdn.net/qq_23387055/article/details/82468387
'''

from sklearn.cluster import KMeans
from sklearn import datasets
from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


ele_file = '../ele.xls' # 电力数据

data = pd.read_excel(ele_file, sheet_name='12021-2') #读取数据

# dbscan = DBSCAN(eps=0.4,min_samples=30)
#
# dbscan.fit(data['FP_TOTALENG'])
#
 #设定不同k值以运算

clf = KMeans(n_clusters=3) #设定k  ！！！！！！！！！！这里就是调用KMeans算法
s = clf.fit(np.array(data['FP_TOTALENG']).reshape(-1,1)) #加载数据集合
numSamples = len(data['FP_TOTALENG'])
centroids = clf.labels_

# 统计各个类别的数目
r1 = pd.Series(clf.labels_).value_counts()
# 找出聚类的中心
r2 = pd.DataFrame(clf.cluster_centers_)
# 横向连接，得到聚类中心对应下的类别下的数目
r = pd.concat([r2,r1],axis=1)

r = pd.concat([data,pd.Series(clf.labels_,index = data.index)],axis=1)
r.columns = list(data.columns) + ['type']
print(r)
mark = ['or', 'ob', 'og', 'ok', '^r', '+r', 'sr', 'dr', '<r', 'pr']
#画出所有样例点 属于同一分类的绘制同样的颜色
plt.figure()

d0 = r[ r['type'] == 0]
print(d0)
plt.plot(d0['TIMESTAMP'], d0['FP_TOTALENG'],c="red", marker='o') #mark[markIndex])

d1 = r[r['type'] == 1]
print(d1)
plt.plot(d1['TIMESTAMP'], d1['FP_TOTALENG'],c="green", marker='o')  # mark[markIndex])

d2 = r[r['type'] == 2]
print(d2)
plt.plot(d2['TIMESTAMP'], d2['FP_TOTALENG'],c="blue", marker='o')  # mark[markIndex])

plt.show()
