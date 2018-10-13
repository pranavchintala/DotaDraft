import json
import logging
import base64
import datetime
import boto3
import time
import ast

#Define DynamoDB Tables
dynamo_db = boto3.resource('dynamodb')
table = dynamo_db.Table('Dota_Test_Only_partition')
teammates_table = dynamo_db.Table('Hero_Teammates')
counters_table = dynamo_db.Table('Hero_Counters')

def invoke_self_async(event, context):
    """
    Have the Lambda invoke itself asynchronously, 
    passing the same event it received originally,
    and tagging the event as 'async' so it's actually processed
    """
    small_event = {}
    small_event['Records'] = []
    small_event_size = 0
    for record in event['Records']:
        record_size = len(bytes(json.dumps(record)))
        if record_size> 129000:
            return
        if small_event_size + record_size < 129000:
            small_event['Records'].append(record)
            small_event_size = small_event_size + record_size
        else:
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
        small_event['async'] = True
        called_function = context.invoked_function_arn
        boto3.client('lambda').invoke(
        FunctionName=called_function,
        InvocationType='Event',
        Payload=bytes(bytes(json.dumps(small_event)))
    )

def lambda_handler(event, context):
    
    if not event.get('async'):
        invoke_self_async(event, context)
        return
    
    def Form_Update_Expression(game):
        update_clause = "SET Matches_Played = if_not_exists(Matches_Played, :init) + :matchinc, Gold_Adv = :gold, XP_Adv = :xp, Kinesis_Timestamp = :kinesis_stamp, Lambda_Timestamp = :lambda_timestamp, DB_Process = :process, "
       
        update_clause = update_clause + "Wins = if_not_exists(Wins, :init) + :wininc, Loss = if_not_exists(Loss, :init) +:loseinc"
        return update_clause

    def Form_Expression_Values(key):
        expression = {':matchinc': key['Matches_Played'], ':wininc': key['Wins'], ':loseinc': key['Matches_Played']-key['Wins'], ':init': 0, ':gold': key['Gold_Adv'], ':xp': key['XP_Adv'], ':process': key['processed'], ':lambda_timestamp': key['Lambda_Timestamp'], ':kinesis_stamp': key['Kinesis_Timestamp']}
        return expression
    def MakeAssociations(list1, list2, ID):
        association=[]
        for dict1 in list1:
            for dict2 in list2:
                if dict1['Hero_ID'] != dict2['Hero_ID']:
                    association.append({'Hero_ID': dict1['Hero_ID'], ID: dict2['Hero_ID'], 'Match_Start_Time': start_time, 'Wins': dict1['Wins'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv'], 'Lambda_Timestamp': lambda_timestamp, 'Kinesis_Timestamp': kinesis_timestamp})
        return association    
    
    
    timestamp = datetime.datetime.utcnow().isoformat()
    
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]

    hero_updates = {}
    teammate_updates = {}
    counter_updates = {}
    
    for info in deserialized_data:
        info['Lambda_Timestamp'] = timestamp
        radiant_item = []
        dire_item = []
        #teammates = []
        #counters = []
        
        if isinstance(info['radiant_xp_adv'][0],basestring):
            radiant_xp_adv = int(info['radiant_xp_adv'][0].split(",")[-1][:-1])
        else:
            radiant_xp_adv = 0
        if isinstance(info['radiant_gold_adv'][0],basestring):
            radiant_gold_adv = int(info['radiant_gold_adv'][0].split(",")[-1][:-1])
        else:
            radiant_gold_adv = 0
        
        players = ast.literal_eval(info['pgroup'][0])
        start_time = info['start_time'][0]
        kinesis_timestamp = info['Kinesis_Stream_Timestamp']
        lambda_timestamp = info['Lambda_Timestamp']
        for key in players:
            hero = str(players[key]['hero_id'])
            
            if info['radiant_win'][0] == "t":
                radiant_w_l = 1
                dire_w_l = 0
            else:
                radiant_w_l = 0
                dire_w_l = 1
            
            if len(key)==1:
                radiant_item.append({'Hero_ID': hero, 'Match_Start_Time': start_time, 'Wins': radiant_w_l, 'Gold_Adv': radiant_gold_adv, 'XP_Adv': radiant_xp_adv,'Lambda_Timestamp': lambda_timestamp, 'Kinesis_Timestamp': kinesis_timestamp})
            else:
                dire_item.append({'Hero_ID': hero, 'Match_Start_Time': start_time, 'Wins': dire_w_l, 'Gold_Adv': -radiant_gold_adv, 'XP_Adv': -radiant_xp_adv, 'Lambda_Timestamp': lambda_timestamp, 'Kinesis_Timestamp': kinesis_timestamp})

        teammates = MakeAssociations(radiant_item,radiant_item,'Teammate_ID')
        for item in MakeAssociations(dire_item,dire_item,'Teammate_ID'):
            teammates.append(item)
        counters = MakeAssociations(radiant_item,dire_item,'Counter_ID')
        for item in MakeAssociations(dire_item,radiant_item,'Counter_ID'):
            counters.append(item)

        '''    
        #Create teammate and counter associations 
        for dict1 in radiant_item:
            for dict2 in radiant_item:
                if dict1['Hero_ID'] != dict2['Hero_ID']:
                    teammates.append({'Hero_ID': dict1['Hero_ID'], 'Teammate_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'Wins': dict1['Wins'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv'], 'Lambda_Timestamp': lambda_timestamp, 'Kinesis_Timestamp': kinesis_timestamp})
        for dict1 in dire_item:
            for dict2 in dire_item:
                if dict1['Hero_ID'] != dict2['Hero_ID']:
                    teammates.append({'Hero_ID': dict1['Hero_ID'], 'Teammate_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'Wins': dict1['Wins'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv'], 'Lambda_Timestamp': lambda_timestamp, 'Kinesis_Timestamp': kinesis_timestamp})
        for dict1 in radiant_item:
            for dict2 in dire_item:
                counters.append({'Hero_ID': dict1['Hero_ID'], 'Counter_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'Wins': dict1['Wins'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv'], 'Lambda_Timestamp': lambda_timestamp, 'Kinesis_Timestamp': kinesis_timestamp})
        for dict1 in dire_item:
            for dict2 in radiant_item:
                counters.append({'Hero_ID': dict1['Hero_ID'], 'Counter_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'Wins': dict1['Wins'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv'], 'Lambda_Timestamp': lambda_timestamp, 'Kinesis_Timestamp': kinesis_timestamp})
        '''
        
        full_game = radiant_item + dire_item
        for item in full_game:
            item['processed'] = datetime.datetime.utcnow().isoformat()
            hero = item['Hero_ID']
            if hero in hero_updates:
                for metric in ['Wins','Gold_Adv','XP_Adv']:
                    hero_updates[hero][metric] += item[metric]
                hero_updates[hero]['Matches_Played'] += 1
            else:
                hero_updates[hero] = {'Wins': item['Wins'], 'Gold_Adv': item['Gold_Adv'], 'XP_Adv': item['XP_Adv'], 'Matches_Played': 1, 'Lambda_Timestamp': item['Lambda_Timestamp'], 'Kinesis_Timestamp': item['Kinesis_Timestamp']}
        
        for item in teammates:
            item['processed'] = datetime.datetime.utcnow().isoformat()
            team_mate = item['Hero_ID'] + ":" + item['Teammate_ID']
            if team_mate in teammate_updates:
                for metric in ['Wins','Gold_Adv','XP_Adv']:
                    teammate_updates[team_mate][metric] += item[metric]
                teammate_updates[team_mate]['Matches_Played'] += 1
            else:
                teammate_updates[team_mate] = {'Wins': item['Wins'], 'Gold_Adv': item['Gold_Adv'], 'XP_Adv': item['XP_Adv'], 'Matches_Played': 1, 'Lambda_Timestamp': item['Lambda_Timestamp'], 'Kinesis_Timestamp': item['Kinesis_Timestamp']}
        
        for item in counters:
            item['processed'] = datetime.datetime.utcnow().isoformat()
            counter_pair = item['Hero_ID'] + ":" + item['Counter_ID']
            if counter_pair in counter_updates:
                for metric in ['Wins','Gold_Adv','XP_Adv']:
                    counter_updates[counter_pair][metric] += item[metric]
                counter_updates[counter_pair]['Matches_Played'] += 1
            else:
                counter_updates[counter_pair] = {'Wins': item['Wins'], 'Gold_Adv': item['Gold_Adv'], 'XP_Adv': item['XP_Adv'], 'Matches_Played': 1, 'Lambda_Timestamp': item['Lambda_Timestamp'], 'Kinesis_Timestamp': item['Kinesis_Timestamp']}
    
    for stat in hero_updates:
        key = hero_updates[stat]
        key['processed'] = datetime.datetime.utcnow().isoformat()
        Update_Expression = Form_Update_Expression(key)
        Expression_Values = Form_Expression_Values(key)
        table.update_item(
                    Key={
                    'Hero_ID': stat
                },
                UpdateExpression = Update_Expression,
                ExpressionAttributeValues = Expression_Values
)
       
    for stat in teammate_updates:
        key = teammate_updates[stat]
        hero_key, teammate_key = stat.split(':')[0], stat.split(':')[1]
        key['processed'] = datetime.datetime.utcnow().isoformat()
        Update_Expression = Form_Update_Expression(key)
        Expression_Values = Form_Expression_Values(key)
        teammates_table.update_item(
                    Key={
                    'Hero_ID': hero_key,
                    'Teammate_ID': teammate_key,
                },
                UpdateExpression = Update_Expression,
                ExpressionAttributeValues = Expression_Values
)
        
    for stat in counter_updates:
        key = counter_updates[stat]
        hero_key, counter_key = stat.split(':')[0], stat.split(':')[1]
        key['processed'] = datetime.datetime.utcnow().isoformat()
        Update_Expression = Form_Update_Expression(key)
        Expression_Values = Form_Expression_Values(key)
        counters_table.update_item(
                    Key={
                    'Hero_ID': hero_key,
                    'Counter_ID': counter_key,
                },
                UpdateExpression = Update_Expression,
                ExpressionAttributeValues = Expression_Values
)
    
    
    
   
