title = ('filename','ID','FileID','TestPointID','DateTime','HandsetDateTime','NetTech','Longitude','Latitude','Altitude','Heading','Speed','MsgID','Category','MsgTitle','MsgBinary','plink','Distance','NRDeployType','NRCAState','SS_RSRP','SS_RSRQ','SS_SINR','PUCCH_TxPower','PUSCH_Power','SRS_TxPower','PreambleTxPower','Pathloss','MCSAvg_DL','MCSBest_DL','MCSMost_DL','MCSAvg_UL','MCSBest_UL','MCSMost_UL','PDCPThr_DL','PDCPThr_UL','RLCThr_DL','RLCThr_UL','MACThr_DL','MACThr_UL','PhyThr_DL','PhyThr_UL','PDSCH_Slots','Num_PDSCH_Slot','PUSCH_Slots','Num_PUSCH_Slot','DL_Grants','Num_DL_Grant','UL_Grants','Num_UL_Grant','CQI_Avg','CQI_Best','CQI_Most','PDSCH_BLER','PDSCH_iBLER','PDSCH_rBLER','PDSCH_Total_Crcfailtb_Cnt','PDSCH_Initial_Crcfailtb_Cnt','PDSCH_Residual_Crcfailtb_Cnt','PUSCH_BLER','PUSCH_iBLER','PUSCH_rBLER','PUSCH_Total_Crcfailtb_Cnt','PUSCH_Initial_Crcfailtb_Cnt','PUSCH_Residual_Crcfailtb_Cnt')

print(title)
print(type(title))
print(str(title))

row = ''
for field in title:
    row += str(field)+','

print(row[:-1])

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("test") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0") \
    .config("spark.debug.maxToStringFields", "500") \
    .getOrCreate()

data = [('A',18),('B',20)]
df = spark.createDataFrame(data, ['name','age'])
print(df.collect())

df_pd = df.toPandas()
print(df_pd)

import pandas as pd
df_www = pd.read_csv("foo.csv", sep="\t")
print(df_www)
print(type(df_www))
