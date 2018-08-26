from __future__ import print_function
from const import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def output(rdd):
    airports = rdd.takeOrdered(10, key = lambda x : -x[1])
    print('----------Top 10 Airport----------')
    for airport in airports:
        print("%s %d" % (airport[0], airport[1]))

def input_preporcess(line):
    fields = line.split(",")
    return ((str(fields[ORIGIN_COL]), 1), (str(fields[DEST_COL]), 1)) if fields[DEST_COL] != "" else None


def updateFunction(newValues, runningCount):
    return sum(newValues) + (runningCount or 0)

if __name__ == '__main__':
    #set up
    sc = SparkContext(appName="q11")
    ssc = StreamingContext(sc, TimeOut)
    brokers = BootStarpServers
    topic = TopicName
    sc.setLogLevel("WARN")
    ssc.checkpoint("/tmp/q11")

    kvs = KafkaUtils.createDirectStream(ssc, [topic], KafkaParams)

    #key logic
    lines = kvs.map(lambda x: x[1])
    rst = lines.flatMap(input_preporcess).filter(lambda x : x != None).updateStateByKey(updateFunction)
    rst.foreachRDD(output)

    #start program
    ssc.start()
    ssc.awaitTermination()
