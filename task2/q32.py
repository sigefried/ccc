from __future__ import print_function
from const import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import boto3
from decimal import *


def parseTime(depTime):
    t = depTime.split(".")[0]
    if len(t) < 4 or int(t[:2]) < 12:
        return 'AM'
    else:
        return 'PM'

targetCandidates = [
    ('BOS', 'ATL', '2008-04-03', 'AM'),
	('ATL', 'LAX', '2008-04-05', 'PM'),
	('PHX', 'JFK', '2008-09-07', 'AM'),
	('JFK', 'MSP', '2008-09-09', 'PM'),
	('DFW', 'STL', '2008-01-24', 'AM'),
	('STL', 'ORD', '2008-01-26', 'PM'),
	('LAX', 'MIA', '2008-05-16', 'AM'),
	('MIA', 'LAX', '2008-05-18', 'PM')
]

def input_preporcess(line):
    fields = line.split(",")
    if fields[FLIGHT_DATE_COL] == 'FlightDate' or fields[ARRDELAY_COL] == '':
        return None
    origin = str(fields[ORIGIN_COL])
    dest = str(fields[DEST_COL])
    flightDate = str(fields[FLIGHT_DATE_COL])
    flightTime = str(fields[DEPTIME_COL])
    flightCarrier = str(fields[UNIQUECARRIER_COL])
    flightNum = str(fields[FLIGHTNUM_COL])
    flightArrDelay = float(fields[ARRDELAY_COL])

    ret = ((origin, dest, flightDate, parseTime(flightTime)), 
            (flightArrDelay, flightCarrier + flightNum))
    return ret if ret[0] in targetCandidates else None


def updateFunction(newValues, currentMin):
	if currentMin is None:
	    currentMin = newValues[0]
	newValues.append(currentMin)
	newMin = min(newValues, key=lambda k: k[0])
	return newMin



if __name__ == '__main__':
    # set up
    sc = SparkContext(appName="q32")
    ssc = StreamingContext(sc, TimeOut)
    brokers = BootStarpServers
    topic = TopicName2008
    sc.setLogLevel("WARN")
    ssc.checkpoint("/tmp/q32")

    kvs = KafkaUtils.createDirectStream(ssc, [topic], KafkaParams)

    # key logic
    def processRDD(rdd):
        print("start processing rdd...")
        rdd.foreachPartition(save_to_dynamoDB)
        print("rdd processed...")
        print("-----------------------------------------------")

    def save_to_dynamoDB(partition):
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('q32')
        print("start processing partition...")
        with table.batch_writer() as batch:
            for rec in partition:
                print(rec)
                src_dest = rec[0][0] + "_" + rec[0][1]
                date = rec[0][2]
                flight = rec[1][1]
                arr_delay = rec[1][0]
                batch.put_item(Item={
                    "src_dest": src_dest,
                    "date" : date,
                    "flight": flight,
                    "arr_delay": Decimal(arr_delay)
                })
        print("partition processing finished...")

    lines = kvs.map(lambda x: x[1])
    rst = lines.map(input_preporcess).filter(
        lambda x: x != None).updateStateByKey(updateFunction)
    rst.foreachRDD(processRDD)

    # start program

    ssc.start()
    ssc.awaitTermination()
