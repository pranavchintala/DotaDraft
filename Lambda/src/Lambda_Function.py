import json
import logging
import base64
import datetime
import boto3
import time

def invoke_self_async(event, context):
    """
    Have the Lambda invoke itself asynchronously, passing the same event it received originally,
    and tagging the event as 'async' so it's actually processed
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("New Invocation")
    small_event = {}
    small_event['Records'] = []
    small_event_size = 0
    for record in event['Records']:
        #logger.info(bytes(json.dumps(record)))
        record_size = len(bytes(json.dumps(record)))
        if record_size> 129000:
            logger.info("Single record too big")
            return
        if small_event_size + record_size < 129000:
            small_event['Records'].append(record)
            small_event_size = small_event_size + record_size
            logger.info("Size Updated to:")
            logger.info(small_event_size)
        else:
            logger.info("About to send:")
            logger.info(small_event_size)
            small_event['async'] = True
            called_function = context.invoked_function_arn
            boto3.client('lambda').invoke(
            FunctionName=called_function,
            InvocationType='Event',
            Payload=bytes(bytes(json.dumps(small_event)))
    )
            small_event['Records'] = []
            small_event_size = 0
            small_event['Records'].append(record)
            small_event_size = small_event_size + record_size
    if small_event_size > 0:
        logger.info("About to send at final:")
        logger.info(small_event_size)
        small_event['async'] = True
        called_function = context.invoked_function_arn
        boto3.client('lambda').invoke(
        FunctionName=called_function,
        InvocationType='Event',
        Payload=bytes(bytes(json.dumps(small_event)))
    )
    
    #event['async'] = True
    
    #called_function = context.invoked_function_arn
    #boto3.client('lambda').invoke(
    #    FunctionName=called_function,
    #    InvocationType='Event',
    #    Payload=bytes(json.dumps(event))
    #)

def Form_Update_Expression(game):
    update_clause = "SET Matches_Played = if_not_exists(Matches_Played, :init) + :inc, W_L = :w_l , Match_Start_Time = :match_start, Gold_Adv = :gold, XP_Adv = :xp, Kinesis_Timestamp = :kinesis_stamp, Lambda_Timestamp = :lambda_timestamp, Process_Time = :process, "
    if game["W_L"] == "Win":
        update_clause = update_clause + "Wins = if_not_exists(Wins, :init) + :inc, Loss = if_not_exists(Loss, :init)"
    elif game["W_L"] == "Loss":
        update_clause = update_clause + "Wins = if_not_exists(Wins, :init), Loss = if_not_exists(Loss, :init) + :inc"
    return update_clause
    
def lambda_handler(event, context):
    
    if not event.get('async'):
        invoke_self_async(event, context)
        return
    
    
    #logger = logging.getLogger()
    #logger.setLevel(logging.INFO)
    dynamo_db = boto3.resource('dynamodb')
    #table = dynamo_db.Table('Dota_Test')
    table = dynamo_db.Table('Dota_Test_Only_partition')
    #for i in range(0,len(event['Records'])):
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]
    #logger.info(len(deserialized_data))
    for info in deserialized_data:
        info['Lambda_Timestamp'] = datetime.datetime.utcnow().isoformat()
        radiant_item = []
        dire_item = []
        teammates = []
        counters = []
        
        if isinstance(info['radiant_xp_adv'][0],basestring):
            radiant_xp_adv = int(info['radiant_xp_adv'][0].split(",")[-1][:-1])
        else:
            radiant_xp_adv = 0
        if isinstance(info['radiant_gold_adv'][0],basestring):
            radiant_gold_adv = int(info['radiant_gold_adv'][0].split(",")[-1][:-1])
        else:
            radiant_gold_adv = 0
        
        fields = info['pgroup'][0].split(",")
        fieldlen = len(fields)-1
        start_time = info['start_time'][0]
        kinesis_timestamp = info['Kinesis_Stream_Timestamp']
        for j in range(1,fieldlen,3):
            hero = fields[j].split(":")[1]
            if j<fieldlen/2:
                if info['radiant_win'][0] == "t":
                    radiant_w_l = "Win"
                else:
                    radiant_w_l = "Loss"
                radiant_item.append({'Hero_ID': hero, 'Match_Start_Time': start_time, 'W_L': radiant_w_l, 'Gold_Adv': radiant_gold_adv, 'XP_Adv': radiant_xp_adv,'Lambda_Timestamp': info['Lambda_Timestamp'], 'Kinesis_Timestamp': kinesis_timestamp})
            else:
                if info['radiant_win'][0] == "t":
                    dire_w_l = "Loss"
                else:
                    dire_w_l = "Win"
                dire_item.append({'Hero_ID': hero, 'Match_Start_Time': start_time, 'W_L': dire_w_l, 'Gold_Adv': -radiant_gold_adv, 'XP_Adv': -radiant_xp_adv, 'Lambda_Timestamp': info['Lambda_Timestamp'], 'Kinesis_Timestamp': kinesis_timestamp})
        
        for dict1 in radiant_item:
            for dict2 in radiant_item:
                if dict1['Hero_ID'] != dict2['Hero_ID']:
                    teammates.append({'Hero_ID': dict1['Hero_ID'], 'Teammate_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv']})
        for dict1 in dire_item:
            for dict2 in dire_item:
                if dict1['Hero_ID'] != dict2['Hero_ID']:
                    teammates.append({'Hero_ID': dict1['Hero_ID'], 'Teammate_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv']})
        for dict1 in radiant_item:
            for dict2 in dire_item:
                counters.append({'Hero_ID': dict1['Hero_ID'], 'Counter_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv']})
        for dict1 in dire_item:
            for dict2 in radiant_item:
                counters.append({'Hero_ID': dict1['Hero_ID'], 'Counter_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv']})
        
        with table.batch_writer() as batch_writer:
            full_game = radiant_item + dire_item
            for item in full_game:
                item['processed'] = datetime.datetime.utcnow().isoformat()
                Update_Expression = Form_Update_Expression(item)
                
                table.update_item(
                    Key={
                    'Hero_ID': item['Hero_ID']
                },
                UpdateExpression = Update_Expression, 
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':init': 0,
                    ':w_l': item['W_L'],
                    ':match_start': item['Match_Start_Time'],
                    ':gold': item['Gold_Adv'],
                    ':xp': item['XP_Adv'],
                    ':process': item['processed'],
                    ':lambda_timestamp': item['Lambda_Timestamp'],
                    ':kinesis_stamp': item['Kinesis_Timestamp'],
                },
                ReturnValues="UPDATED_NEW"
)
            
                #batch_writer.put_item(Item=kinesis_record)
        #logger.info("Written")
        #logger.info(teammates)
    # TODO implement
    import json
import logging
import base64
import datetime
import boto3
import time

def invoke_self_async(event, context):
    """
    Have the Lambda invoke itself asynchronously, passing the same event it received originally,
    and tagging the event as 'async' so it's actually processed
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("New Invocation")
    small_event = {}
    small_event['Records'] = []
    small_event_size = 0
    for record in event['Records']:
        #logger.info(bytes(json.dumps(record)))
        record_size = len(bytes(json.dumps(record)))
        if record_size> 129000:
            logger.info("Single record too big")
            return
        if small_event_size + record_size < 129000:
            small_event['Records'].append(record)
            small_event_size = small_event_size + record_size
            logger.info("Size Updated to:")
            logger.info(small_event_size)
        else:
            logger.info("About to send:")
            logger.info(small_event_size)
            small_event['async'] = True
            called_function = context.invoked_function_arn
            boto3.client('lambda').invoke(
            FunctionName=called_function,
            InvocationType='Event',
            Payload=bytes(bytes(json.dumps(small_event)))
    )
            small_event['Records'] = []
            small_event_size = 0
            small_event['Records'].append(record)
            small_event_size = small_event_size + record_size
    if small_event_size > 0:
        logger.info("About to send at final:")
        logger.info(small_event_size)
        small_event['async'] = True
        called_function = context.invoked_function_arn
        boto3.client('lambda').invoke(
        FunctionName=called_function,
        InvocationType='Event',
        Payload=bytes(bytes(json.dumps(small_event)))
    )
    
    #event['async'] = True
    
    #called_function = context.invoked_function_arn
    #boto3.client('lambda').invoke(
    #    FunctionName=called_function,
    #    InvocationType='Event',
    #    Payload=bytes(json.dumps(event))
    #)

def Form_Update_Expression(game):
    update_clause = "SET Matches_Played = if_not_exists(Matches_Played, :init) + :inc, W_L = :w_l , Match_Start_Time = :match_start, Gold_Adv = :gold, XP_Adv = :xp, Kinesis_Timestamp = :kinesis_stamp, Lambda_Timestamp = :lambda_timestamp, Process_Time = :process, "
    if game["W_L"] == "Win":
        update_clause = update_clause + "Wins = if_not_exists(Wins, :init) + :inc, Loss = if_not_exists(Loss, :init)"
    elif game["W_L"] == "Loss":
        update_clause = update_clause + "Wins = if_not_exists(Wins, :init), Loss = if_not_exists(Loss, :init) + :inc"
    return update_clause
    
def lambda_handler(event, context):
    
    if not event.get('async'):
        invoke_self_async(event, context)
        return
    
    
    #logger = logging.getLogger()
    #logger.setLevel(logging.INFO)
    dynamo_db = boto3.resource('dynamodb')
    #table = dynamo_db.Table('Dota_Test')
    table = dynamo_db.Table('Dota_Test_Only_partition')
    #for i in range(0,len(event['Records'])):
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]
    #logger.info(len(deserialized_data))
    for info in deserialized_data:
        info['Lambda_Timestamp'] = datetime.datetime.utcnow().isoformat()
        radiant_item = []
        dire_item = []
        teammates = []
        counters = []
        
        if isinstance(info['radiant_xp_adv'][0],basestring):
            radiant_xp_adv = int(info['radiant_xp_adv'][0].split(",")[-1][:-1])
        else:
            radiant_xp_adv = 0
        if isinstance(info['radiant_gold_adv'][0],basestring):
            radiant_gold_adv = int(info['radiant_gold_adv'][0].split(",")[-1][:-1])
        else:
            radiant_gold_adv = 0
        
        fields = info['pgroup'][0].split(",")
        fieldlen = len(fields)-1
        start_time = info['start_time'][0]
        kinesis_timestamp = info['Kinesis_Stream_Timestamp']
        for j in range(1,fieldlen,3):
            hero = fields[j].split(":")[1]
            if j<fieldlen/2:
                if info['radiant_win'][0] == "t":
                    radiant_w_l = "Win"
                else:
                    radiant_w_l = "Loss"
                radiant_item.append({'Hero_ID': hero, 'Match_Start_Time': start_time, 'W_L': radiant_w_l, 'Gold_Adv': radiant_gold_adv, 'XP_Adv': radiant_xp_adv,'Lambda_Timestamp': info['Lambda_Timestamp'], 'Kinesis_Timestamp': kinesis_timestamp})
            else:
                if info['radiant_win'][0] == "t":
                    dire_w_l = "Loss"
                else:
                    dire_w_l = "Win"
                dire_item.append({'Hero_ID': hero, 'Match_Start_Time': start_time, 'W_L': dire_w_l, 'Gold_Adv': -radiant_gold_adv, 'XP_Adv': -radiant_xp_adv, 'Lambda_Timestamp': info['Lambda_Timestamp'], 'Kinesis_Timestamp': kinesis_timestamp})
        
        for dict1 in radiant_item:
            for dict2 in radiant_item:
                if dict1['Hero_ID'] != dict2['Hero_ID']:
                    teammates.append({'Hero_ID': dict1['Hero_ID'], 'Teammate_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv']})
        for dict1 in dire_item:
            for dict2 in dire_item:
                if dict1['Hero_ID'] != dict2['Hero_ID']:
                    teammates.append({'Hero_ID': dict1['Hero_ID'], 'Teammate_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv']})
        for dict1 in radiant_item:
            for dict2 in dire_item:
                counters.append({'Hero_ID': dict1['Hero_ID'], 'Counter_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv']})
        for dict1 in dire_item:
            for dict2 in radiant_item:
                counters.append({'Hero_ID': dict1['Hero_ID'], 'Counter_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv']})
        
        with table.batch_writer() as batch_writer:
            full_game = radiant_item + dire_item
            for item in full_game:
                item['processed'] = datetime.datetime.utcnow().isoformat()
                Update_Expression = Form_Update_Expression(item)
                
                table.update_item(
                    Key={
                    'Hero_ID': item['Hero_ID']
                },
                UpdateExpression = Update_Expression, 
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':init': 0,
                    ':w_l': item['W_L'],
                    ':match_start': item['Match_Start_Time'],
                    ':gold': item['Gold_Adv'],
                    ':xp': item['XP_Adv'],
                    ':process': item['processed'],
                    ':lambda_timestamp': item['Lambda_Timestamp'],
                    ':kinesis_stamp': item['Kinesis_Timestamp'],
                },
                ReturnValues="UPDATED_NEW"
)
            
                #batch_writer.put_item(Item=kinesis_record)
        #logger.info("Written")
        #logger.info(teammates)
    # TODO implement
    
