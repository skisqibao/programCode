import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
from datetime import datetime, timedelta

# 参数正常取值范围表
df_threshold = pd.read_csv('aidiag_script/param_threshold.csv')
df_threshold = df_threshold[df_threshold['use'] == 1].copy()

# 将带有时间戳的实时解码参数数据整理成按秒采样的参数数据，范围为异常时间点前后4分钟
def ts_sample(df_raw: DataFrame, time_point: datetime) -> DataFrame:
    # 时间戳转换为DateTime
    df_raw['DateTime'] = pd.to_datetime(df_raw['DateTime'], format='%Y-%m-%d %H:%M:%S')
    
    # 列过滤
    df_temp = df_raw[['DateTime']].copy()
    for name in df_threshold['param']:
        if name in df_raw.columns:
            df_temp[name] = df_raw[name].copy()
        else:
            df_temp[name] = np.NaN
            print(name, '【参数缺失】')
    df_raw = df_temp.copy()
    
    # 空值处理
    df_raw.replace(['None', -10000, '-10000', '-10000.0', '-10000.00'], [np.NaN, np.NaN, np.NaN, np.NaN, np.NaN], inplace=True)
    
    # 参数值转换成float64
    param_names = df_threshold['param'].values
    df_raw[param_names] = df_raw[param_names].astype(np.float64)
    
    # 根据阈值过滤无效数据
    for i in range(len(df_threshold)):
        row = df_threshold.iloc[i]
        param = row['param']
        min_value = row['min']
        max_value = row['max']

        small_rows = df_raw[param] < min_value
        large_rows = df_raw[param] > max_value

        if sum(small_rows) > 0:
            print(param + ' 小于阈值范围' + str(min_value) + ':', df_raw[small_rows][param].values)
            df_raw.loc[small_rows, param] = np.NaN
        if sum(large_rows) > 0:
            print(param + ' 大于阈值范围' + str(max_value) + ':', df_raw[large_rows][param].values)
            df_raw.loc[large_rows, param] = np.NaN
    
    # 时间处理，精确到秒
    df_raw['sec'] = df_raw['DateTime'].apply(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
    df_raw['sec'] = pd.to_datetime(df_raw['sec'], format='%Y-%m-%d %H:%M:%S')
    
    # 创建异常时间点前后4分钟的时间序列，每秒一帧
    start_sec = time_point - timedelta(seconds=240)
    end_sec = time_point + timedelta(seconds=240)
    full_sec = pd.date_range(start_sec, end_sec, freq='S', closed='left')
    
    # 按秒采样，如果该秒有多个数值则取平均值，如果该秒无值则留空
    df_param = pd.merge(DataFrame({'sec': full_sec}), df_raw, how='left')
    df_param = df_param.groupby('sec').mean().sort_index()
    df_param.reset_index(drop=True, inplace=True)
    
    return df_param
