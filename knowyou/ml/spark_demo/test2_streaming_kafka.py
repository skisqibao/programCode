import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream
from pyspark.streaming.kafka import KafkaUtils

# 确定环境变量以及引入kafka对应包
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 ' \
    'pyspark-shell'


def updataFunc(channel, actualChannel):
    if actualChannel is None:
        actualChannel = 0
    return sum(channel, actualChannel)


conf = SparkConf().setMaster("yarn-client").setAppName("test-pyspark-kafka")
# sc写成这样是因为在jupyter直接运行总是因sc已存在而报ValueError
try:
    sc = SparkContext(conf=conf)
except ValueError:
    pass

sc.setLogLevel("WARN")
scc = StreamingContext(sc, 240)
# checkpoint一定要设置，否则报错
scc.checkpoint("hdfs://172.16.1.222:8020/spark/checkpoint")

# Kafka相关的配置
zookeper = "172.16.1.220:2181;172.16.1.222:2181;192.168.1.33:2181;192.168.1.34:2181"
kfk_brokers = {"bootstrap_servers": "172.16.1.222:6667",
               "kafka.bootstrap.servers": "172.16.1.222:6667",
               "brokers": "172.16.1.222:6667",
               "host": "172.16.1.222:6667"}

topic = {"test": 1}
group_id = "test"

lines = KafkaUtils.createStream(scc, zookeper, group_id, topic, kafkaParams=kfk_brokers)
KafkaUtils.createRDD()
print(lines)

lineTmp = lines.map(lambda x: x[1])

scc.start()
scc.awaitTermination()
