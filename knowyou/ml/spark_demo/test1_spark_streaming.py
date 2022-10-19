from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# 创建Streaming上下文，指定batch间隔1秒
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

# 创建DStream来连接主机端口
linesDStream = ssc.socketTextStream("172.16.1.220", 9997)

words = linesDStream.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
