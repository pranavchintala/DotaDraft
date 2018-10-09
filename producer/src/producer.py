import boto3
from io import BytesIO
import json
import sys
import time

def get_kinesis_record(item):
       # item = {'hashKey': randrange(0, 5000000), 'sortKey': str(uuid.uuid4()), 'created': datetime.datetime.utcnow().isoformat()}

        raw_data = json.dumps(item)
        encoded_data = bytes(raw_data)
        kinesis_record = {
            'Data': encoded_data,
            'PartitionKey': str(item['hashKey']),
        }

        return kinesis_record





client = boto3.client(
's3',
aws_access_key_id=AWS_ACCESS_KEY,
aws_secret_access_key=AWS_SECRET_KEY)


client = boto3.client('s3')

kinesis_client = boto3.client('kinesis', region_name='us-east-1')
print kinesis_client.describe_stream(StreamName = "TestStream")
print "Collecting data from bucket..."
#datafroms3 = client.get_object(Bucket = 'dotadatastorage', Key = 'match_skill.gz')
datafroms3 = client.get_object(Bucket = 'dotadatastorage', Key = 'matches')
print type(datafroms3['Body'])
print "DONE"
i=1
print "Starting Streaming!"
while True:
	bytestream = datafroms3['Body'].read(amt=512)
	print bytestream
	put_response = kinesis_client.put_record(StreamName = "TestStream", Data = bytestream,PartitionKey = str(i))
	i=i+1
	if i>500:
		i=1
'''
while True:
	bytestream = datafroms3['Body'].iter_lines(chunk_size=10)
	for line in bytestream:
		print line
		put_response = kinesis_client.put_record(StreamName = "TestStream", Data = line,PartitionKey = str(i))
		i=i+1
	if i>500:
		i=1
	time.sleep(1)
        print "Sending stream to kinesis"
        #put_response = kinesis_client.put_record(StreamName = "TestStream", Data = bytestream,PartitionKey = str(i))
        #print put_response
'''
