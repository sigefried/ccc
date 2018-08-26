import boto3

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# Instantiate a table resource object without actually
# creating a DynamoDB table. Note that the attributes of this table
# are lazy-loaded: a request is not made nor are the attribute
# values populated until the attributes
# on the table resource are accessed or its load() method is called.
table = dynamodb.Table('q21')

# Print out some data about the table.
# This will cause a request to be made to DynamoDB and its attribute
# values will be set based on the response.
print(table.creation_date_time)

# single put
table.put_item( Item={ 'airport' : 'test', 'carrier' : 3333, 'test': 'aaaa' })

# batched write
inputs = [{'airport' : 'test1', 'carrier' : 23333, 'test': 'baaaa' },
 {'airport' : 'test2', 'carrier' : 31333, 'test': 'baaaa' },
 {'airport' : 'test3', 'carrier' : 333313, 'test': 'baaaa' }]


def dynamodb_batch_write(items):
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

print "test_batch_write"
dynamodb_batch_write(inputs)
