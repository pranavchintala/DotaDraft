# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import sys
from hero_mappings import heroes
import plotly.plotly as py
import plotly.graph_objs as go
from plotly import tools as tools

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
hero_options=[]
for item in heroes:
    hero_options.append({'label': heroes[item], 'value': str(item)})

app.layout = html.Div(children=[
    html.H1(
        children='DotaDraft',
        style={
	    'font-size': '120px',
            'textAlign': 'center',
            'color': '#e41a1c'
        }
    ),

    dcc.Dropdown(
        id='input-1-state',
	options=hero_options,
        value='41',
	style=dict(width=250)
    ),
    html.Div(id='output-state')
])

@app.callback(Output('output-state', 'children'),
	      [Input('input-1-state', 'value')],
              [State('input-1-state', 'value')])
def update_output(n_clicks,input1):
    colorscheme = '#4daf4a'
    fontsize=18
    
    Hero_Win_Rate = getDynamoWinRate(input1)    
    print Hero_Win_Rate
    Heroes_Team,Matches_Team,Wins_Team = getDynamoData(input1,'Hero_Teammates','Teammate_ID')
    Heroes_Counter,Matches_Counter,Wins_Counter = getDynamoData(input1,'Hero_Counters','Counter_ID')
    
    trace1 = go.Scatter(
    x=Heroes_Team,
    y=Wins_Team,
    name='Win Percentage'
    )
    trace2 = go.Scatter(
    x=Heroes_Team,
    y=Matches_Team,
    name='Matches Played Together',
    yaxis='y2'
    )
    trace3 = go.Scatter(
    x=Heroes_Counter,
    y=Wins_Counter,
    name='Win Percentage'
    )
    trace4 = go.Scatter(
    x=Heroes_Counter,
    y=Matches_Counter,
    name='Matches Played Against',
    yaxis='y2'
    )

    data = [trace1, trace2]
    data2 = [trace3, trace4]
    
    layout = go.Layout(
    legend=dict(orientation="h",font=dict(size=18,color=colorscheme)),
    title='Hero Teammates for ' + heroes[int(input1)],
    titlefont=dict(size=24,color=colorscheme),
    xaxis=dict(
        title='Hero Name',
	titlefont=dict(
            color=colorscheme,
	    size=fontsize
        ),
        tickfont=dict(
            color=colorscheme,
	    size=fontsize
        )
    ),
    yaxis=dict(
        title='Win %',
	titlefont=dict(
            color=colorscheme,
	    size=fontsize
        ),
        tickfont=dict(
            color=colorscheme,
    	    size=fontsize
        )
    ),
    yaxis2=dict(
        title='Number of Matches',
        titlefont=dict(
            color=colorscheme,
	    size=fontsize
        ),
        tickfont=dict(
            color=colorscheme,
	    size=fontsize
        ),
        overlaying='y',
        side='right',
	)
    )
    
    colorscheme='#c51b8a'
    layout2 = go.Layout(
    legend=dict(orientation="h",font=dict(size=18,color=colorscheme)),
    title='Hero Counters for ' + heroes[int(input1)],
    titlefont=dict(size=24,color=colorscheme),
    xaxis=dict(
        title='Hero Name',
	titlefont=dict(
            color=colorscheme,
	    size=fontsize
        ),
        tickfont=dict(
            color=colorscheme,
            size=fontsize
        )
    ),
    yaxis=dict(
        title='Win %',
	titlefont=dict(
            color=colorscheme,
            size=fontsize
        ),
        tickfont=dict(
            color=colorscheme,
            size=fontsize
        )
    ),
    yaxis2=dict(
        title='Number of Matches',
        titlefont=dict(
            color=colorscheme,
            size=fontsize
        ),
        tickfont=dict(
            color=colorscheme,
            size=fontsize
        ),
        overlaying='y',
        side='right'
        )
    )

    fig = go.Figure(data=data, layout=layout)
    fig2 = go.Figure(data=data2, layout=layout2)
    return html.Div([
    dcc.Graph(figure=fig, id='my-figure'),
    dcc.Graph(figure=fig2, id='my-figure-2')
    ]) 
def getDynamoWinRate(input_hero):
    dynamodb = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url="http://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table("Hero_Statistics")

    try:
        response = table.query(
        KeyConditionExpression=Key('Hero_ID').eq(input_hero)
    )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
	data = response['Items']
	win_rate = data[0]['Wins']/data[0]['Matches_Played']
	return "{:.2%}".format(win_rate)

def getDynamoData(input_hero,table_name,id_type):
    dynamodb = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url="http://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table(table_name)


    try:
        response = table.query(
        KeyConditionExpression=Key('Hero_ID').eq(input_hero)
    )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
	data  = response['Items']
        data_sorted = sorted(data, key = lambda k: k['Wins']/k['Matches_Played'], reverse=True)
	top_picks = []
	if table_name=='Hero_Teammates':
		top_picks = data_sorted[:5]
	elif table_name=='Hero_Counters':
		top_picks = data_sorted[-5:]
		top_picks.reverse()
        Heroes = []
        Matches_Played = []
        Wins = []
        for item in top_picks:
            item['Win_Rate'] = "{:.2%}".format(item['Wins']/item['Matches_Played'])
            Heroes.append(heroes[int(item[id_type])])
            Matches_Played.append(item['Matches_Played'])
            Wins.append(item['Win_Rate'])
        return (Heroes, Matches_Played, Wins)


if __name__ == '__main__':
    app.run_server(host='ec2-18-235-156-64.compute-1.amazonaws.com', debug=True,port=80)



