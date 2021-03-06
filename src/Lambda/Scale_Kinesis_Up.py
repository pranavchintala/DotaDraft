import json
import boto3

kinesis = boto3.client('kinesis')
cw = boto3.client('cloudwatch')

#From AWS Documentation
def lambda_handler(event, context):
    message = json.loads(event['Records'][0]['Sns']['Message'])
    
    stream_name = 'Consumer_Test'
    stream = kinesis.describe_stream(
                                     StreamName=stream_name
                                     )
        
                                     #Determine total number of shards in the stream
                                     totalShardCount = len(stream['StreamDescription']['Shards'])
                                     lastShardId = stream['StreamDescription']['Shards'][totalShardCount - 1]['ShardId']
                                     while(stream['StreamDescription']['HasMoreShards']):
                                         stream = kinesis.describe_stream(
                                                                          StreamName=stream_name,
                                                                          ExclusiveStartShardId=lastShardId
                                                                          )
                                             currentShardCount = len(stream['StreamDescription']['Shards'])
                                             totalShardCount += currentShardCount
                                                 lastShardId = stream['StreamDescription']['Shards'][currentShardCount - 1]['ShardId']

#Double the shard count in the stream
kinesis.update_shard_count(
                           StreamName=stream_name,
                           TargetShardCount=totalShardCount * 2,
                           ScalingType='UNIFORM_SCALING'
                           )
    
    #Double the threshold for the CloudWatch alarm that triggered this function
    cw.put_metric_alarm(
                        AlarmName=message['AlarmName'],
                        AlarmActions=[event['Records'][0]['Sns']['TopicArn']],
                        MetricName=message['Trigger']['MetricName'],
                        Namespace=message['Trigger']['Namespace'],
                        Statistic=message['Trigger']['Statistic'].title(),
                        Dimensions=[
                                    {
                                    'Name': message['Trigger']['Dimensions'][0]['name'],
                                    'Value': message['Trigger']['Dimensions'][0]['value']
                                    }
                                    ],
                        Period=message['Trigger']['Period'],
                        EvaluationPeriods=message['Trigger']['EvaluationPeriods'],
                        Threshold=message['Trigger']['Threshold'] * 2,
                        ComparisonOperator=message['Trigger']['ComparisonOperator']
                        )
