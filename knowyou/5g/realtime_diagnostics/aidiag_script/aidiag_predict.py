from numpy import ndarray, argmax
from sklearn.externals import joblib

# 载入已经训练好的模型
clf = joblib.load('aidiag_script/aidiag_model.pkl')

# 将时序特征输入已经训练好的模型，得到预测的故障原因
def predict(feature: ndarray) -> dict:
    assert feature.shape[0] == 1
    
    # 获取模型预测结果，每个分类的概率分布
    pred = clf.predict_proba(feature)[0]
    
    # 故障原因中文名称
    reason_name = ['其他', '弱覆盖']
    
    # 模型预测的故障原因概率分布
    pred_dict = {
        'predict_reason': reason_name[argmax(pred)],
        'predict_reason_prob': {reason_name[1]: pred[1], reason_name[0]: pred[0]}
    }
    
    return pred_dict
