import datetime
import random
import time
import boto3


kinesis_client = boto3.client('kinesis', region_name='us-east-1')
kinesis_stream_name = "Requests_Stream"

while True:
	picks = []
	#We want a single game stream of requests to go to the same shard to make it easier for caching the previous reads!
	Partition = str(random.randrange(0,50000))
	while len(picks)<9:
		random_hero = random.randint(0,112)
		#We cannot have duplicate heroes in the same game
		while random_hero in picks:
			random_hero = random.randint(0,112)
		picks.append(random_hero)
		time.sleep(.3)
		put_response = kinesis_client.put_record(StreamName = kinesis_stream_name, PartitionKey = Partition, Data = str(picks))
