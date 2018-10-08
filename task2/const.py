BootStarpServers = "localhost:9092"
zookeepers = "localhost:2181"
# BootStarpServers = "ip-172-30-0-10.ec2.internal:9092"
# zookeepers = "ip-172-30-0-10.ec2.internal:2181,ip-172-30-0-59.ec2.internal:2181,ip-172-30-0-156.ec2.internal:2181"
TopicName = "performance"
TopicName2008 = "performance2008"
TimeOut = 3
FLIGHT_DATE_COL = 0
UNIQUECARRIER_COL = 1
FLIGHTNUM_COL = 2
ORIGIN_COL = 3
DEST_COL = 4
DEPTIME_COL = 5
DEPDELAY_COL = 6
ARRTIME_COL = 7
ARRDELAY_COL = 8

KafkaParams = {"bootstrap.servers": BootStarpServers,
               "auto.offset.reset": "smallest",
               "group.id": "ccc-group2"
               }
