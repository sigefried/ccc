#! /usr/bin/python
import boto3
import pprint
from boto3.dynamodb.conditions import Key, Attr

airports = [ "SRQ", "CMH", "JFK", "SEA", "BOS"]

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('q21')

for airport in airports:
    print("-----------%s------------" % airport)
    response = table.query(
        KeyConditionExpression=Key('airport').eq(airport)
    )
    rst = []
    for item in response['Items']:
        rst.append((float(item[u'avg_dep_delay']), str(item[u'carrier'])))
    rst.sort()
    pprint.pprint(rst[:10])

