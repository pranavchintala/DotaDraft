# DotaDraft
This project aims to provide real-time draft decision making assistance for the popular E-sport Dota 2. 
##### (From Wikipedia):
Dota 2 is a multiplayer online battle arena video game developed and published by Valve Corporation. The game is played in matches between 
two teams of five players, with each team occupying and defending their own separate base on the map. 
Each of the ten players independently controls a powerful character, known as a "hero".

To begin the game, ten players must take turns selecting unique heroes from a large pool, each of which possess unique
abilities and differing styles of play. DotaDraft focuses on assisting in this procedure, which is termed "drafting."

Dataset used:
OpenDOTA provides a data dump of historical matches, of which I have utilized ~1.2TB in order to simulate a stream of incoming matches. 
For each match it contains information such as start time, match metadata, and data for each particular player.

As the official Steam API comes with query limits of ~100,000 calls per day, OpenDOTA is used alternatively. 
To simulate real-time data streaming, the raw data in csv format is converted to JSON format before being ingested by Amazon Kinesis.

# Business Value

1. Dota 2 is notorious for having one of the highest learning curves of any game, which makes it difficult and frustrating for inexperienced players
to participate. By utlizing DotaDraft to empower them in choosing more appropriate heroes for certain situations, we can alleviate this effect. 

2. OpenAI, a non-profit AI research company, has invested heavily (on the verge of two year's worth of resources) into creating intelligent bots 
capable of competing against professionals in Dota 2. While these bots have proven their skill in 
playing the game at a high level, they lack the capability of drafting. DotaDraft combined with deep learning techniques 
could potentially give them the edge in this regard. 

3. Dota 2 boasts the highest prize pool on record for E-sports in the world: The International 8 tournament prize pool held in August 2018
was ~$25 million. Teams and coaches are leveraging many advanced methods to improve their drafts and consequently, their win rates at the 
highest level. 

# Application
