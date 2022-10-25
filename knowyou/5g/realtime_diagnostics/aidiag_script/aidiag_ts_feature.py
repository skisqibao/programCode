import tsfel
from pandas.core.frame import DataFrame
from pandas import concat

# 创建统计特征与时序特征提取工具
cfg_statistical = tsfel.get_features_by_domain('statistical')
cfg_temporal = tsfel.get_features_by_domain('temporal')

# 从转换后的参数数据中，提取统计特征与时序特征
def ts_feature(df_data: DataFrame) -> DataFrame:
    print('ts_feature begin')
    df_statistical = tsfel.time_series_features_extractor(cfg_statistical, df_data, verbose=0)
    df_temporal = tsfel.time_series_features_extractor(cfg_temporal, df_data, verbose=0)
    
    # 横向合并统计特征与时序特征
    df_feature = concat([df_statistical, df_temporal], axis=1)
    
    print('ts_feature success')
    
    return df_feature
