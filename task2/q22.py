from __future__ import print_function
from const import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import boto3
from decimal import *
from collections import defaultdict
import heapq




def input_preporcess(line):
    fields = line.split(",")
    if fields[FLIGHT_DATE_COL] == 'FlightDate' or fields[DEPDELAY_COL] == '':
        return None
    origin = str(fields[ORIGIN_COL])
    carrier = str(fields[UNIQUECARRIER_COL])
    dep_delay = float(fields[DEPDELAY_COL])
    return ((origin, carrier), dep_delay)


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
    sc = SparkContext(appName="q22")
    ssc = StreamingContext(sc, TimeOut)
    brokers = BootStarpServers
    topic = TopicName
    sc.setLogLevel("WARN")
    ssc.checkpoint("/tmp/q22")

    kvs = KafkaUtils.createDirectStream(ssc, [topic], KafkaParams)

    # key logic
    def processRDD(rdd):
        print("start processing rdd...")
        rdd.foreachPartition(save_to_dynamoDB)
        print("rdd processed...")
        print("-----------------------------------------------")

    def save_to_dynamoDB(partition):
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('q22')
        print("start processing partition...")
        pq_map = defaultdict(list)
        for rec in partition:
            airport = rec[0][0]
            carrier = rec[0][1]
            avg_dep_delay = rec[1][0] / rec[1][1]
            pq = pq_map[airport]
            heapq.heappush(pq,(-avg_dep_delay, carrier))
            if len(pq) > 10: 
                heapq.heappop(pq)
        
        for k, pq in pq_map.items():

        # with table.batch_writer() as batch:
        #     for k, pq in pq_map.items():
        #         for it in pq:
        #             batch.put_item(Item={
        #                 "airport": k,
        #                 "carrier": it[1],
        #                 "avg_dep_delay": Decimal(str(-it[0]))
        #             })
        print("partition processing finished...")

    lines = kvs.map(lambda x: x[1])
    rst = lines.map(input_preporcess).filter(
        lambda x: x != None).updateStateByKey(updateFunction)
    rst.foreachRDD(processRDD)




        
    # start program

    ssc.start()
    ssc.awaitTermination()
