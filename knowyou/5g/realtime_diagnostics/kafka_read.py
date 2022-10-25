# _*_coding:utf-8_*_
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType

import os
dirname = os.path.split(os.path.realpath(__file__))[0]

"""
    读取kafka实时数据
    处理数据
    写入本地文件
"""


def getInputSchema():
    decode_schema = StructType() \
        .add("uuid", StringType()) \
        .add("device_sn", StringType()) \
        .add("nodetype", StringType()) \
        .add("datasource", StringType()) \
        .add("testtime", StringType()) \
        .add("ss_rsrp", StringType()) \
        .add("ss_rsrq", StringType()) \
        .add("ss_sinr", StringType()) \
        .add("pucch_txpower", StringType()) \
        .add("pusch_txpower", StringType()) \
        .add("dlmcsavg", StringType()) \
        .add("dlmcsbest", StringType()) \
        .add("dlmcsmost", StringType()) \
        .add("ulmcsavg", StringType()) \
        .add("ulmcsbest", StringType()) \
        .add("ulmcsmost", StringType()) \
        .add("macthr_dl", StringType()) \
        .add("macthr_ul", StringType()) \
        .add("phythr_dl", StringType()) \
        .add("pdsch_slots", StringType()) \
        .add("pusch_slots", StringType()) \
        .add("dl_grants", StringType()) \
        .add("ul_grants", StringType()) \
        .add("cqi_avg", StringType()) \
        .add("cqi_best", StringType()) \
        .add("cqi_most", StringType()) \
        .add("pdsch_bler", StringType()) \
        .add("pusch_bler", StringType()) \
        .add("band", StringType()) \
        .add("bandwith", StringType()) \
        .add("earfcndl", StringType()) \
        .add("pci", StringType()) \
        .add("tac", StringType()) \
        .add("cellid", StringType()) \
        .add("slottimingpattern", StringType()) \
        .add("specialslotsymbolpattern", StringType()) \
        .add("referencesubcarrierspacing", StringType()) \
        .add("networktype", StringType()) \
        .add("networkstate", StringType()) \
        .add("timesec", StringType()) \
        .add("device_id", StringType()) \
        .add("scene_id", StringType()) \
        .add("dt", StringType()) \
        .add("dt_hour", StringType()) \
        .add("dt_min", StringType()) \
        .add("dt_sec", StringType()) \
        .add("net_type", StringType()) \
        .add("p_scene_id", StringType())

    exception_schema = StructType() \
        .add("uuid", StringType()) \
        .add("deviceSn", StringType()) \
        .add("dataType", StringType()) \
        .add("dtSec", StringType())

    return decode_schema, exception_schema


if __name__ == '__main__':
    # 初始化spark配置
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("real-time diagnostics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    decode_schema, exception_schema = getInputSchema()
    # kafkaBrokers = "172.16.1.222:6667"
    kafkaBrokers = "192.168.1.102:26667"
    decodeTopic = "fvgiis-wireless-Message"
    exceptionTopic = "fvgiis-ai-alarm"
    write_path = 'file://' + os.path.join(dirname, 'kafka_train_df')
    checkpoint_path = 'file://' + os.path.join(dirname, 'ck1')

    # 连接kafka生产者并加载正常数据
    decodeDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafkaBrokers) \
        .option("subscribe", decodeTopic) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", decode_schema).alias("data")) \
        .selectExpr("data.uuid uuid1", "data.device_sn", "data.nodetype", "data.datasource", "data.testtime",
                    "data.ss_rsrp", "data.ss_rsrq", "data.ss_sinr",
                    "data.pucch_txpower", "data.pusch_txpower",
                    "data.dlmcsavg", "data.dlmcsbest", "data.dlmcsmost",
                    "data.ulmcsavg", "data.ulmcsbest", "data.ulmcsmost",
                    "data.cqi_avg", "data.cqi_best", "data.cqi_most",
                    "data.pdsch_bler", "data.pusch_bler",
                    "data.timesec", "data.device_id", "data.scene_id",
                    "data.dt", "data.dt_hour", "data.dt_min",
                    "CAST(from_unixtime(unix_timestamp(data.dt_sec, 'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) dt_sec",
                    "data.net_type", "data.p_scene_id") \
        .filter("device_sn not in ('ecf039c0-dd2c-41e8-ae9a-a7289326e61b')") \
        .withWatermark("dt_sec", "1 hour")

    exceptionDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafkaBrokers) \
        .option("subscribe", exceptionTopic) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", exception_schema).alias("data")) \
        .selectExpr("data.uuid", "data.deviceSn",
                    "CAST(from_unixtime(unix_timestamp(data.dtSec, 'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) dtSec") \
        .filter("deviceSn not in ('ecf039c0-dd2c-41e8-ae9a-a7289326e61b')") \
        .withWatermark("dtSec", "1 hour")

    joinDF = decodeDF.join(
        exceptionDF,
        F.expr("""
            uuid1 = uuid and 
            device_sn = deviceSn and
            (unix_timestamp(dtSec, 'yyyy-MM-dd HH:mm') - unix_timestamp(dt_sec, 'yyyy-MM-dd HH:mm')) <= 240 and
            (unix_timestamp(dt_sec, 'yyyy-MM-dd HH:mm') - unix_timestamp(dtSec, 'yyyy-MM-dd HH:mm')) <= 240 
        """)
    )

    trainDF = joinDF.selectExpr("CAST(dt_sec AS STRING) dt_sec",
                                "ss_rsrp SS_RSRP", "ss_rsrq SS_RSRQ", "ss_sinr SS_SINR",
                                "pucch_txpower PUCCH_TxPower", "pusch_txpower PUSCH_TxPower",
                                "'' Pathloss",
                                "dlmcsavg MCSAvg_DL", "dlmcsbest MCSBest_DL", "dlmcsmost MCSMost_DL",
                                "ulmcsavg MCSAvg_UL", "ulmcsbest MCSBest_UL", "ulmcsmost MCSMost_UL",
                                "cqi_avg CQI_Avg", "cqi_best CQI_Best", "cqi_most CQI_Most",
                                "pdsch_bler PDSCH_BLER", " '' PDSCH_iBLER", "'' PDSCH_rBLER",
                                "pusch_bler PUSCH_BLER", "'' PUSCH_iBLER", "'' PUSCH_rBLER",
                                "uuid", "deviceSn device_sn", "CAST(dtSec AS STRING) abnormal_sec"
                                )

    # trainDF.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .trigger(processingTime='6 seconds') \
    #     .start() \
    #     .awaitTermination()


    trainDF.writeStream \
        .format("csv") \
        .outputMode("append") \
        .trigger(processingTime='60 seconds') \
        .option("path", write_path) \
        .option("checkpointLocation", checkpoint_path) \
        .start() \
        .awaitTermination()

