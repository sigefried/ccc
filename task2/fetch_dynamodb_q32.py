#! /usr/bin/python
import boto3
import pprint
from boto3.dynamodb.conditions import Key, Attr

pairs = [
        [
            ('BOS', 'ATL', '2008-04-03', 'AM'),
            ('ATL', 'LAX', '2008-04-05', 'PM')
            ],
        [
            ('PHX', 'JFK', '2008-09-07', 'AM'),
            ('JFK', 'MSP', '2008-09-09', 'PM')
            ],
        [
            ('DFW', 'STL', '2008-01-24', 'AM'),
            ('STL', 'ORD', '2008-01-26', 'PM')
            ],
        [
            ('LAX', 'MIA', '2008-05-16', 'AM'),
            ('MIA', 'LAX', '2008-05-18', 'PM')
            ]
        ]

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('q32')

for pair in pairs:
    print(pair)
    print("=========================")
    print("Route: %s->%s->%s" % (pair[0][0], pair[0][1], pair[1][1]))
    print("1st Departure Date: " + pair[0][2])
    print("2nd Departure Date: " + pair[1][2] + "\n")
    key_one = pair[0][0] + "_" + pair[0][1]
    key_two = pair[1][0] + "_" + pair[1][1]
    response = table.query(
         KeyConditionExpression=Key('src_dest').eq(key_one)
    )
    rst_one = response['Items'][0]
    delay_one = rst_one[u'arr_delay']
    response = table.query(
         KeyConditionExpression=Key('src_dest').eq(key_two)
    )
    rst_two = response['Items'][0]
    delay_two = rst_two[u'arr_delay']
    print(" [Flight 1]")
    print(" Flight: " + rst_one[u'flight'])
    print(" Arrival Delay: " + str(delay_one) + "\n")
    print(" [Flight 2]")
    print(" Flight: " + rst_two[u'flight'])
    print(" Arrival Delay: " + str(delay_two) + "\n")
    print(" Total Delay: " + str(delay_one + delay_two))
