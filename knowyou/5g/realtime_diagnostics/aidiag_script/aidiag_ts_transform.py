from pandas.core.frame import DataFrame

# 对按秒采样的参数数据进行进一步数据转换处理
def ts_transform(df_param: DataFrame) -> DataFrame:
    print('ts_transform begin')
    df_data = df_param.copy()
    
    # 进行线性插值处理，两端的缺失值延伸补齐
    df_data.interpolate(method='linear', limit_direction='both', inplace=True)
    
    print('ts_transform success')
    
    return df_data
