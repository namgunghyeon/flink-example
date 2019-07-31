import boto3
import json
from datetime import datetime
import calendar
import random
import time


my_stream_name = 'TextInputStream2'

kinesis_client = boto3.client('kinesis', region_name='ap-northeast-1',
                              aws_access_key_id='',
                              aws_secret_access_key='')

data = [
    "{\"movieId\":\"123\",\"userId\":\"1\",\"timestamp\":_time}",
    "{\"movieId\":\"123\",\"userId\":\"2\",\"timestamp\":_time}",
    "{\"movieId\":\"123\",\"userId\":\"3\",\"timestamp\":_time}",

    "{\"movieId\":\"123\",\"userId\":\"1\",\"timestamp\":_time}",
    "{\"movieId\":\"123\",\"userId\":\"1\",\"timestamp\":_time}",
    "{\"movieId\":\"123\",\"userId\":\"1\",\"timestamp\":_time}",

    "{\"movieId\":\"123\",\"userId\":\"1\",\"timestamp\":_time}",
    "{\"movieId\":\"123\",\"userId\":\"1\",\"timestamp\":_time}",
    "{\"movieId\":\"123\",\"userId\":\"1\",\"timestamp\":_time}",

]

def put_to_stream(thing_id, value, property_timestamp):
    print (value.replace("_time", property_timestamp))
    put_response = kinesis_client.put_record(
                        StreamName=my_stream_name,
                        Data=value.replace("_time", property_timestamp),
                        PartitionKey=thing_id)

while True:
    for value in data:
        property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
        thing_id = 'aa-bb'

        put_to_stream(thing_id, value, str(property_timestamp * 1000))

        # wait for 5 second
    time.sleep(1.5)
