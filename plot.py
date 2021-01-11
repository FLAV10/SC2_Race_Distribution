import pymongo
from pymongo import MongoClient
import requests
import pprint
import time
import numpy as np
import json
from bson import json_util 
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

#create pymongo client 
client = pymongo.MongoClient()

mmr_db = client.mmr_db

pipeline = [
	#{'$project': {'id': '$id', 'race': '$race', 'mmr': '$mmr', 'length': {'$size': '$race'}}},
	#{"$sort": {'length': -1}},
	{'$unwind': '$race'},
	{'$unwind': '$race.race'},
	{"$sort": {'mmr': -1}},
	{'$project': {'mmr': '$mmr', 'race': '$race.race'}},
	{'$project': {'_id':0}}
	#{'$limit': 10}
]

pipeline_terran = [
	{'$unwind': '$race'},
	{'$unwind': '$race.race'},
	{'$sort': {'mmr': -1}},
	{'$match': {'race': 'Terran'}},
	{'$project': {'mmr': '$mmr', 'race': '$race.race'}},
	{'$project': {'count':0}},
	{'$project': {'_id':0}},
	{'$project': {'race':0}}
]

pipeline_protoss = [
	{'$unwind': '$race'},
	{'$unwind': '$race.race'},
	{"$sort": {'mmr': -1}},
	{'$match': {'race': 'Protoss'}},
	{'$project': {'mmr': '$mmr', 'race': '$race.race'}},
	{'$project': {'count':0}},
	{'$project': {'_id':0}},
	{'$project': {'race':0}}
]

pipeline_zerg = [
	{'$unwind': '$race'},
	{'$unwind': '$race.race'},
	{"$sort": {'mmr': -1}},
	{'$match': {'race': 'Zerg'}},
	{'$project': {'mmr': '$mmr', 'race': '$race.race'}},
	{'$project': {'count':0}},
	{'$project': {'_id':0}},
	{'$project': {'race':0}}
]


#raw_data = list(dist_db.ladders.find({}, projection=exclude_data))
extracted_terran = list(mmr_db.ladders.aggregate(pipeline_terran))
df_terran = pd.DataFrame(extracted_terran)

extracted_protoss = list(mmr_db.ladders.aggregate(pipeline_protoss))
df_protoss = pd.DataFrame(extracted_protoss)

extracted_zerg = list(mmr_db.ladders.aggregate(pipeline_zerg))
df_zerg = pd.DataFrame(extracted_zerg)


pprint.pprint(df_terran)
pprint.pprint(df_protoss)
pprint.pprint(df_zerg)


distribution = sns.distplot(df_terran, hist=False, color = 'b', axlabel='MMR', rug=True, label='Terran')
distribution = sns.distplot(df_zerg, hist=False, color = 'r', axlabel='MMR', rug=True, label='Zerg')
distribution = sns.distplot(df_protoss, hist=False, color = 'y', axlabel='MMR', rug=True, label='Protoss')

distribution.set(xlim=(1000,6000))
distribution.set_ylabel('Probability Density')
distribution.set_title('NA SC2 LoTV 1v1 Racial Distribution Season 37 - only 1 game')


plt.show()