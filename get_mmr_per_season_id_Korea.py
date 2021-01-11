import pymongo
from pymongo import MongoClient
import requests
import pprint
import time
import numpy as np
import json

#instructions ----- set season id to current id and run. 

# get current season = https://kr.api.battle.net/data/sc2/season/current?locale=ko_KR&access_token=yv7ajx74qgefkefdpwjkmqvn
# get specific season = https://kr.api.battle.net/data/sc2/league/28/201/0/5?locale=ko_KR&access_token=yv7ajx74qgefkefdpwjkmqvn
# Queue ID 201 legacy of the void 1v1. 
# Team type 0 for matched
# league id 0-6 for bronze to GM
# get mmr for ladder id = https://kr.api.battle.net/data/sc2/ladder/69546?locale=ko_KR&access_token=yv7ajx74qgefkefdpwjkmqvn

#set access token and keys
access_token = '&access_token=yv7ajx74qgefkefdpwjkmqvn'
key = '&apikey=3wc2xd4ajqhcgkj8utw22sf847yp98g2'
secret = 'ZFAeU7p5yfxg8NaqHn4mgjXgH73GgX37'

#define api endpoint and parameters
season_call = 'https://kr.api.battle.net/data/sc2/league'
season_id = 37 #this should be the 10-2018 season
lotv_1v1_ladder_code = 201
match_made_code = 0
#0-6 is bronze to GM
league_code = 0
eng_locale = '?locale=ko_KR'
slash = '/'
season_call_url = season_call+slash+str(season_id)+slash+str(lotv_1v1_ladder_code)+slash+str(match_made_code)+slash+str(league_code)+eng_locale+access_token

#define url and reponse codes and just stringify things for good measure
url = str(season_call_url)
bad_r = requests.get('http://httpbin.org/status/404')
good_r = requests.get('http://httpbin.org/status/200')
wait_r = requests.get('http://httpbin.org/status/504')


#create pymongo client 
client = pymongo.MongoClient()

#drop and define the db 
season37_ladders_db = client.season_ladders_db
season37_ladders_db.ladders.drop()
season37_ladders_db = client.season_ladders_db

#populate db with ladder numbers from a season id across all leagues

for league_code in range(7):
	season_call_url = season_call+slash+str(season_id)+slash+str(lotv_1v1_ladder_code)+slash+str(match_made_code)+slash+str(league_code)+eng_locale+access_token
	url = str(season_call_url)
	response = requests.get(url)
	print('trying league '+str(league_code)+' in season '+str(season_id))
	print('calling :'+url)
	if response.status_code == 200:
		print('status: '+str(response))
		season37_ladders_db.ladders.insert_one(response.json())
	else:
		for attempt in range(5):
			if response.status_code != 200:
				print(str(response)+'retrying attempt #'+str(attempt+1)+' of 5')
				time.sleep(.5)
			else:
				print('attempt #'+str(attempt+1)+' SUCCESSFUL!')
				season37_ladders_db.ladders.insert_one(response.json())
		else:
			print('Error: response timed out')
			break

#test db population with printout
for ladders in season37_ladders_db.ladders.find():
	pprint.pprint(ladders)	

#define db
db_ids = client.db_ids
db_ids.ladders.drop()
db_ids = client.db_ids


#A pipeline that takes season data and pulls the 'ladder_id's from the giant mess
pipeline = [
	{'$unwind': '$tier'},
	{'$unwind': '$tier.division'},
	{'$project': {'ladder_id': '$tier.division.ladder_id'}},
	{'$project': {'_id':0}}
]

#test the pipeline
pprint.pprint(list(season37_ladders_db.ladders.aggregate(pipeline)))
print('testing pipeline by running aggregate on season37_ladders_db')

#create a new db of just unique ids and ladders from the season
for ladders in season37_ladders_db.ladders.aggregate(pipeline):
	db_ids.ladders.insert_one(ladders)


#test that the database of just ladder id's populated
for ladders in db_ids.ladders.find():
	pprint.pprint(ladders)

print('the ladder_id database has that many entries --->'+str(db_ids.ladders.count()))
	
#Turn the database into an array of ladders	
ladder_array = np.array([])

ladder_array = db_ids.ladders.distinct('ladder_id')

pprint.pprint(ladder_array)

#define mmr database

mmr_db = client.mmr_db
mmr_db.ladders.drop()


#define api call parameters
# get mmr for ladder id = https://kr.api.battle.net/data/sc2/ladder/271302?locale=ko_KR&access_token=yv7ajx74qgefkefdpwjkmqvn
ladder_call = 'https://kr.api.battle.net/data/sc2/ladder'
ladder_call_no_data = 'https://kr.api.battle.net/sc2/ladder'

count = 0 
#populate a database with all of the ladder results
for ladder_number in ladder_array:
	ladder_call_url = ladder_call+slash+str(ladder_number)+eng_locale+access_token
	url = str(ladder_call_url)
	response = requests.get(url)
	response_json = response.json()
	print('trying ladder number '+str(ladder_number)+'... call number '+str(count)+' of '+str(len(ladder_array)))
	print('calling :'+url)
	if response.status_code == 200:
		print('status: '+str(response))
		count = count + 1
		member_int = len(response_json['team'])-1
		while member_int >= 0: 
			id = str(response_json['team'][member_int]['id'])
			legacy_id = str(response_json['team'][member_int]['member'][0]['legacy_link']['id'])
			mmr = response_json['team'][member_int]['rating']
			race = response_json['team'][member_int]['member'][0]['played_race_count']
			post = {'id': id,
				'legacy_id': legacy_id,
				'mmr': mmr,
				'race': race}
			mmr_db.ladders.insert_one(post)
			member_int = member_int - 1
	else:
		for attempt in range(5):
			if response.status_code != 200:
				print(str(response)+'retrying attempt #'+str(attempt+1)+' of 5')
				time.sleep(.5)
			else:
				print('attempt #'+str(attempt+1)+' SUCCESSFUL!')
				member_int = len(response_json['team'])-1
				while member_int >= 0: 
					id = str(response_json['team'][member_int]['id'])
					legacy_id = str(response_json['team'][member_int]['member'][0]['legacy_link']['id'])
					mmr = response_json['team'][member_int]['rating']
					race = response_json['team'][member_int]['member'][0]['played_race_count']
					post = {'id': id,
						'legacy_id': legacy_id,					
						'mmr': mmr,
						'race': race}
					mmr_db.ladders.insert_one(post)
					member_int = member_int - 1
		else:
			print('Error: response timed out')
			break

#define playerid database


mmr_db.races.drop()

			
#populate a database with all of the player ids and 
for ladder_number in ladder_array:
	ladder_call_url = ladder_call_no_data+slash+str(ladder_number)+eng_locale+key
	url = str(ladder_call_url)
	response = requests.get(url)
	response_json = response.json()
	print('trying ladder number '+str(ladder_number)+'... call number '+str(count)+' of '+str(len(ladder_array)))
	print('calling :'+url)
	if response.status_code == 200:
		print('status: '+str(response))
		count = count + 1
		member_int = len(response_json['ladderMembers'])-1
		while member_int >= 0: 
			try:	
				id = str(response_json['ladderMembers'][member_int]['character']['id'])
				race = str(response_json['ladderMembers'][member_int]['favoriteRaceP1'])
				post = {'id': id, 'race': race}
				mmr_db.races.insert_one(post)
				member_int = member_int - 1
			except KeyError: 
				print('KeyError ... Continue')
				member_int = member_int - 1
				continue
	else:
		for attempt in range(5):
			if response.status_code != 200:
				print(str(response)+'retrying attempt #'+str(attempt+1)+' of 5')
				time.sleep(.5)
			else:
				print('attempt #'+str(attempt+1)+' SUCCESSFUL!')
				member_int = len(response_json['ladderMembers'])-1
				while member_int >= 0: 
					try:
						id = str(response_json['ladderMembers'][member_int]['character']['id'])
						race = response_json['ladderMembers'][member_int]['favoriteRaceP1']
						post = {'id': id, 'race': race}
						mmr_db.races.insert_one(post)
						member_int = member_int - 1
					except KeyError: 
						print('KeyError ... Continue')
						member_int = member_int - 1
						continue
		else:
			print('Error: response timed out')
			break
			
# print the completion news
pprint.pprint(mmr_db.ladders.find_one())
pprint.pprint(mmr_db.races.find_one())
print('the database of mmrs is complete for season'+str(season_id)+'this is a printout to prove it')	
