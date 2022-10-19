from sklearn.datasets import load_iris
from sklearn import tree

from sklearn2pmml.pipeline import PMMLPipeline
from sklearn2pmml import sklearn2pmml


# 数据加载处理
X, y = load_iris(return_X_y=True)

# 训练模型
clf = tree.DecisionTreeClassifier()
clf = clf.fit(X, y)

# tree.plot_tree(clf)

# a = clf.predict([[1., 2., 3., 4.]])
# b = clf.predict([[4., 3., 2., 1.]])
# print(a, b)

# 导出到PMML文件
# pipeline = PMMLPipeline([("classifier", tree.DecisionTreeClassifier())])
# result = pipeline.fit(X, y)
# sklearn2pmml(pipeline, './iris.pmml', with_repr=True)

# 操作kafka获取4分钟的数据


# 时间特征提取


# 带入模型预测


# 预测结果写回

