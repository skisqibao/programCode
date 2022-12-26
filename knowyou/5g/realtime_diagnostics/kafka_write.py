# _*_coding:utf-8_*_
from pyspark.sql import SparkSession
from aidiag_script.aidiag_predict_phase import predict_phase
from aidiag_script.aidiag_ts_sample import ts_sample

import sys
import pandas as pd
import os
import json
import sqlite3
from datetime import datetime

dirname = os.path.split(os.path.realpath(__file__))[0]

"""
    读取本地csv文件,提取处理数据
    数据带入模型预测
    结果数据写入kafka
"""


def getSourceDataFrame(data_path):
    """
        读取实时流输出csv文件,并将读取的文件删除
    :param data_path: ./realtime_diagnostics/kafka_train_df
    :return: Pandas.Dataframe
    """
    all_file_list = os.listdir(data_path)
    all_csv_list = []
    labels = ['DateTime', 'SS_RSRP', 'SS_RSRQ', 'SS_SINR', 'PUCCH_TxPower', 'PUSCH_TxPower', 'Pathloss', 'MCSAvg_DL',
              'MCSBest_DL', 'MCSMost_DL', 'MCSAvg_UL', 'MCSBest_UL', 'MCSMost_UL', 'CQI_Avg', 'CQI_Best', 'CQI_Most',
              'PDSCH_BLER', 'PDSCH_iBLER', 'PDSCH_rBLER', 'PUSCH_BLER', 'PUSCH_iBLER', 'PUSCH_rBLER', 'uuid',
              'device_sn', 'abnormal_sec']
    for single_file in all_file_list:
        if single_file.endswith(".csv"):
            all_csv_list.append(single_file)

    for single_csv in all_csv_list:
        single_df = pd.read_csv(os.path.join(data_path, single_csv), names=labels)
        # print(single_df)
        if single_csv == all_csv_list[0]:
            source_df = single_df
        else:
            source_df = pd.concat([source_df, single_df], ignore_index=True)

    return source_df


if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    # 初始化spark配置
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("real-time diagnostics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    conn = sqlite3.connect(os.path.join(dirname, 'diagnostics.db'))
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS diagnostics(
       id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
       uuid TEXT NOT NULL,
       device_sn TEXT NOT NULL,
       abnormal_sec  TEXT NOT NULL
       );
    ''')

    # kafka_brokers = "172.16.1.222:6667"
    kafka_brokers = "192.168.1.102:26667"
    # write_topic = "test"
    write_topic = "ai-diagnostics-result"
    data_path = os.path.join(dirname, 'kafka_train_df')
    params = ['DateTime', 'SS_RSRP', 'SS_RSRQ', 'SS_SINR', 'PUCCH_TxPower', 'PUSCH_TxPower', 'MCSAvg_DL', 'MCSBest_DL',
              'MCSMost_DL', 'MCSAvg_UL', 'MCSBest_UL', 'MCSMost_UL', 'CQI_Avg', 'CQI_Best', 'CQI_Most', 'PDSCH_BLER',
              'PUSCH_BLER']
    source_df = getSourceDataFrame(data_path)
    print('read kafka write data:\n', source_df)
    write_list = []
    tuple_list = []

    uuid_list = source_df['uuid'].unique()
    for uuid in uuid_list:
        # 按照uuid唯一拆分成多个DataFrame
        unique_uuid_df = source_df[source_df['uuid'].isin([uuid])]

        abnormal_sec_list = unique_uuid_df['abnormal_sec'].unique()
        for abnormal_sec in abnormal_sec_list:
            # 按照abnormal_sec唯一拆分成多个DataFrame
            unique_uuid_sec_df = unique_uuid_df[unique_uuid_df['abnormal_sec'].isin([abnormal_sec])]
            train_df = pd.DataFrame(unique_uuid_sec_df, columns=params)

            # 查询数据, 查询为空执行预测
            cursor = c.execute("SELECT * FROM diagnostics WHERE uuid = ? AND abnormal_sec = ?", (uuid, abnormal_sec))
            if len(cursor.fetchall()) == 0:
                sample_df = ts_sample(train_df, datetime.strptime(abnormal_sec, '%Y-%m-%d %H:%M:%S'))
                print(sample_df)
                result = predict_phase(sample_df)
                print(result)

                device_sn = unique_uuid_sec_df['device_sn'].unique()[0]
                predict_reason = result['predict_reason']
                predict_reason_prob = str(result['predict_reason_prob'])
                keys = ['uuid', 'device_sn', 'abnormal_sec', 'predict_reason', 'predict_reason_prob']
                values = [uuid, device_sn, abnormal_sec, predict_reason, predict_reason_prob]
                dictionary = dict(zip(keys, values))
                output = json.dumps(dictionary, ensure_ascii=False)

                write_list.append(output)
                tuple_list.append((uuid, device_sn, abnormal_sec))

    print('1111', write_list)
    if len(write_list) != 0:
        print('22222')
        spark.createDataFrame(pd.DataFrame({'value': write_list})).select("value").show()
        print('33333')
        spark.createDataFrame(pd.DataFrame({'value': write_list})) \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_brokers) \
            .option("topic", write_topic) \
            .save()

    spark.stop()

    for (uuid, device_sn, abnormal_sec) in tuple_list:
        # 插入数据
        c.execute("INSERT INTO diagnostics (uuid, device_sn, abnormal_sec) VALUES (?, ?, ?)",
                  (uuid, device_sn, abnormal_sec))

        conn.commit()
    conn.close()
