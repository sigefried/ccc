from __future__ import print_function
from const import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def output(rdd):
    rst = rdd.take(10)
    print(rst)

if __name__ == '__main__':
  sc = SparkContext(appName="testSparkStreaming")
  ssc = StreamingContext(sc, TimeOut)
  brokers = BootStarpServers
  topic = TopicName
  sc.setLogLevel("WARN")
  
  
  kvs = KafkaUtils.createDirectStream(ssc, [topic], KafkaParams)
  lines = kvs.map(lambda x: x[1])
  lines.count().pprint()
  
  ssc.start()
  ssc.awaitTermination()
