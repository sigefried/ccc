from __future__ import print_function
from const import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def output(rdd):
    airlines = rdd.takeOrdered(10, key=lambda x: x[1][0] / x[1][1])
    print('----------Top 10 OnTime Arrival Airline----------')
    for airline in airlines:
        print("%s %f" % (airline[0], airline[1][0] * 1.0 / airline[1][1]))


def input_preporcess(line):
    fields = line.split(",")
    if fields[FLIGHT_DATE_COL] == 'FlightDate' or fields[ARRDELAY_COL] == '':
        return None
    return (str(fields[UNIQUECARRIER_COL]), float(fields[ARRDELAY_COL]))


def updateFunction(newValues, runningCount):
    current = (sum(newValues), len(newValues))
    if not runningCount:
        runningCount = current
    else:
        runningCount = (runningCount[0] + current[0],
                        runningCount[1] + current[1])
    return runningCount


if __name__ == '__main__':
    # set up
    sc = SparkContext(appName="q12")
    ssc = StreamingContext(sc, TimeOut)
    brokers = BootStarpServers
    topic = TopicName
    sc.setLogLevel("WARN")
    ssc.checkpoint("/tmp/q12")

    kvs = KafkaUtils.createDirectStream(ssc, [topic], KafkaParams)

    # key logic
    lines = kvs.map(lambda x: x[1])
    rst = lines.map(input_preporcess).filter(
        lambda x: x != None).updateStateByKey(updateFunction)
    rst.foreachRDD(output)

    # start program
    ssc.start()
    ssc.awaitTermination()
