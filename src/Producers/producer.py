import boto3
import json
import time
import random
import pandas
import datetime

def isValidGame(game_modes,human_players,duration):
    if game_modes[0] in (1,2,3,5,16,22) and human_players[0] == 10 and duration[0]>780:
	return True
    return False
def generate_kinesis_record(item):
	JSON = json.dumps(info)
	return JSON


client = boto3.client(
's3',
aws_access_key_id=AWS_ACCESS_KEY,
aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


kinesis_client = boto3.client('kinesis', region_name='us-east-1')
client = boto3.client('s3')
kinesis_stream_name = "Consumer_Test"

#Put records in one at a time into the Kinesis Stream. One thread provides ~100 games per second
for line in pandas.read_csv("s3://dotadatastorage/matches", chunksize = 1):
	info = line.to_dict(orient = "list")
	info["Kinesis_Stream_Timestamp"] = datetime.datetime.utcnow().isoformat()
	game_validity = isValidGame(info['game_mode'],info['human_players'],info['duration'])
	if game_validity:
		#We use a partition key which results in uniform partitioning into Kinesis
		put_response = kinesis_client.put_record(StreamName = kinesis_stream_name, PartitionKey = str(random.randrange(0, 5000000)), Data = generate_kinesis_record(info))
