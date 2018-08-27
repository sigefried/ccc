from __future__ import print_function
from const import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import boto3
from decimal import *




def input_preporcess(line):
    fields = line.split(",")
    if fields[FLIGHT_DATE_COL] == 'FlightDate' or fields[ARRDELAY_COL] == '':
        return None
    origin = str(fields[ORIGIN_COL])
    dest = str(fields[DEST_COL])
    return (origin + "_" + dest, float(fields[ARRDELAY_COL]))


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
    sc = SparkContext(appName="q24")
    ssc = StreamingContext(sc, TimeOut)
    brokers = BootStarpServers
    topic = TopicName
    sc.setLogLevel("WARN")
    ssc.checkpoint("/tmp/q24")

    kvs = KafkaUtils.createDirectStream(ssc, [topic], KafkaParams)

    # key logic
    def processRDD(rdd):
        print("start processing rdd...")
        rdd.foreachPartition(save_to_dynamoDB)
        print("rdd processed...")
        print("-----------------------------------------------")

    def save_to_dynamoDB(partition):
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('q24')
        print("start processing partition...")
        with table.batch_writer() as batch:
            for rec in partition:
                avg_arr_dealy = str(rec[1][0] / rec[1][1])
                src_dest = rec[0]
                batch.put_item(Item={
                    "src_dest": src_dest,
                    "avg_arr_delay": Decimal(avg_arr_dealy)
                })
        print("partition processing finished...")

    lines = kvs.map(lambda x: x[1])
    rst = lines.map(input_preporcess).filter(
        lambda x: x != None).updateStateByKey(updateFunction)
    rst.foreachRDD(processRDD)




        
    # start program

    ssc.start()
    ssc.awaitTermination()
