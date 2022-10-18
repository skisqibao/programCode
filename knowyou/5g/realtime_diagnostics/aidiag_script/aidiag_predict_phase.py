from pandas.core.frame import DataFrame
from aidiag_script.aidiag_ts_transform import ts_transform
from aidiag_script.aidiag_ts_feature import ts_feature
from aidiag_script.aidiag_predict import predict

# 预测全流程处理
# 输入：异常时间点前后4分钟内按秒采样的参数数据（DataFrame对象）
# 输出：模型预测的故障原因概率分布(dict)
# 输出数据样例：
# {
#     "predict_reason": "弱覆盖",
#     "predict_reason_prob": {
#         "弱覆盖": 0.6,
#         "干扰": 0.3,
#         "其他": 0.1,
#     }
# }
def predict_phase(df_param: DataFrame) -> dict:
    # 帧数必须为480
    assert df_param.shape[0] == 480
    
    # 对按秒采样的参数数据进行进一步数据转换处理
    df_data = ts_transform(df_param)
    
    # 从转换后的参数数据中，提取统计特征与时序特征
    df_feature = ts_feature(df_data)
    feature = df_feature.values
    
    # 将时序特征输入已经训练好的模型，得到预测的故障原因
    pred_dict = predict(feature)
    
    return pred_dict
