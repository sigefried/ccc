#! /usr/bin/python
import boto3
import pprint
from boto3.dynamodb.conditions import Key, Attr

airports = [ "LGA_BOS", "BOS_LGA", "OKC_DFW", "MSP_ATL"]

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('q24')

for pair in airports:
    print("-----------%s------------" % pair)
    response = table.query(
        KeyConditionExpression=Key('src_dest').eq(pair)
    )
    rst = []
    for item in response['Items']:
        rst.append((pair, float(item[u'avg_arr_delay'])))
    pprint.pprint(rst)

