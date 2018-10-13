from kinesis.consumer import KinesisConsumer
import sys
import json
import logging
import base64
import datetime
import boto3
import time

def Form_Update_Expression(game):
    update_clause = "SET Matches_Played = if_not_exists(Matches_Played, :init) + :inc, W_L = :w_l , Match_Start_Time = :match_start, Gold_Adv = :gold, XP_Adv = :xp, Kinesis_Timestamp = :kinesis_stamp, Process_Time = :process, "
    if game["W_L"] == "Win":
        update_clause = update_clause + "Wins = if_not_exists(Wins, :init) + :inc, Loss = if_not_exists(Loss, :init)"
    elif game["W_L"] == "Loss":
        update_clause = update_clause + "Wins = if_not_exists(Wins, :init), Loss = if_not_exists(Loss, :init) + :inc"
    return update_clause

def Form_Expression_Values():
	expression = {':inc': 1, ':init': 0, ':w_l': item['W_L'], ':match_start': item['Match_Start_Time'], ':gold': item['Gold_Adv'], ':xp': item['XP_Adv'], ':process': item['processed'], ':kinesis_stamp': item['Kinesis_Timestamp']}
	return expression
		

#Define DynamoDB Tables
dynamo_db = boto3.resource('dynamodb')
table = dynamo_db.Table('Parallel_Test')    
teammates_table = dynamo_db.Table('Hero_Teammates')
counters_table = dynamo_db.Table('Hero_Counters')

#Initialize Kinesis Stream to read from
my_stream_name = 'Consumer_Test'

consumer = KinesisConsumer(stream_name = my_stream_name)   
for rec in consumer:
	info = json.loads(rec['Data'])
	radiant_item = []
        dire_item = []
        teammates = []
        counters = []

	#If Gold/Xp advantages are missing, use 0 as a placeholder 
        if isinstance(info['radiant_xp_adv'],basestring):
            radiant_xp_adv = int(info['radiant_xp_adv'].split(",")[-1][:-1])
        else:
            radiant_xp_adv = 0
        if isinstance(info['radiant_gold_adv'][0],basestring):
            radiant_gold_adv = int(info['radiant_gold_adv'][0].split(",")[-1][:-1])
        else:
            radiant_gold_adv = 0
	
	#Begin parsing for hero relations: Split into two teams to begin        
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
                radiant_item.append({'Hero_ID': hero, 'Match_Start_Time': start_time, 'W_L': radiant_w_l, 'Gold_Adv': radiant_gold_adv, 'XP_Adv': radiant_xp_adv,'Kinesis_Timestamp': kinesis_timestamp})
            else:
                if info['radiant_win'][0] == "t":
                    dire_w_l = "Loss"
                else:
                    dire_w_l = "Win"
                dire_item.append({'Hero_ID': hero, 'Match_Start_Time': start_time, 'W_L': dire_w_l, 'Gold_Adv': -radiant_gold_adv, 'XP_Adv': -radiant_xp_adv, 'Kinesis_Timestamp': kinesis_timestamp})
        
	#Create teammate and counter associations 
        for dict1 in radiant_item:
            for dict2 in radiant_item:
                if dict1['Hero_ID'] != dict2['Hero_ID']:
                    teammates.append({'Hero_ID': dict1['Hero_ID'], 'Teammate_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv'], 'Kinesis_Timestamp': kinesis_timestamp})
        for dict1 in dire_item:
            for dict2 in dire_item:
                if dict1['Hero_ID'] != dict2['Hero_ID']:
                    teammates.append({'Hero_ID': dict1['Hero_ID'], 'Teammate_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv'], 'Kinesis_Timestamp': kinesis_timestamp})
        for dict1 in radiant_item:
            for dict2 in dire_item:
                counters.append({'Hero_ID': dict1['Hero_ID'], 'Counter_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv'], 'Kinesis_Timestamp': kinesis_timestamp})
        for dict1 in dire_item:
            for dict2 in radiant_item:
                counters.append({'Hero_ID': dict1['Hero_ID'], 'Counter_ID': dict2['Hero_ID'], 'Match_Start_Time': start_time, 'W_L': dict1['W_L'], 'Gold_Adv': dict1['Gold_Adv'], 'XP_Adv': dict1['XP_Adv'], 'Kinesis_Timestamp': kinesis_timestamp})
        
        full_game = radiant_item + dire_item
	
	#Write to DynamoDB
        for item in full_game:
            item['processed'] = datetime.datetime.utcnow().isoformat()
            Update_Expression = Form_Update_Expression(item)
	    print Update_Expression
	    Expression_Values = Form_Expression_Values()                
	    print Expression_Values
	    table.update_item(
                    Key={
                    'Hero_ID': item['Hero_ID']
                },
                UpdateExpression = Update_Expression, 
		ExpressionAttributeValues = Expression_Values
)
	for item in teammates:
	    item['processed'] = datetime.datetime.utcnow().isoformat()
	    Update_Expression = Form_Update_Expression(item)
	    print Update_Expression
	    Expression_Values = Form_Expression_Values()
	    print Expression_Values
	    teammates_table.update_item(
		    Key={
		    'Hero_ID': item['Hero_ID'],
		    'Teammate_ID': item['Teammate_ID']
		},
		UpdateExpression = Update_Expression,
		ExpressionAttributeValues = Expression_Values
)
	for item in counters:
            item['processed'] = datetime.datetime.utcnow().isoformat()
            Update_Expression = Form_Update_Expression(item)
            print Update_Expression
            Expression_Values = Form_Expression_Values()
            print Expression_Values
            print "COUNTERS"
            counters_table.update_item(
                    Key={
                    'Hero_ID': item['Hero_ID'],
                    'Counter_ID': item['Counter_ID']
                },
                UpdateExpression = Update_Expression,
                ExpressionAttributeValues = Expression_Values
)

