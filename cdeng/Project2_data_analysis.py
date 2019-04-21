## This python file is under cdeng file

import json
import dml
import prov.model
import datetime
import numpy as np
import uuid
# from Project2_data_analysis_helper import Data_analysis_helper

from random import shuffle
from math import sqrt

'''
This data analysis part shall deal with the relationships between:
	bike stations' # of docks and outgoing trip,
	bike stations' # of docks and incoming trip
	outgoing and incoming trip

Will calculate the correlation coefficients and p-value.
'''


class Project2_data_analysis(dml.Algorithm):
	contributor = 'cdeng'
	reads = ['cdeng.stations_popular_start', 'cdeng.stations_popular_end']
	writes = ['cdeng.stations_dock_incoming_outgoing_stats']

	@staticmethod
	def execute(trial = False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		# (username, password)
		repo.authenticate('cdeng', 'cdeng')

		################################### Begin Operations here ###################################
		print('Algorithm 4: Data analysis for project 2')
		print('NOTICE: the trial mode will take around 8-10 seconds to run while the normal mode will take around 16-20 seconds to run.')
		helper = Data_analysis_helper()

		if trial:
			print('Trial mode...')
			permutation_size=2000
		else:
			permutation_size=5000

		print('Loading data for analysis...')
		# Grab data
		station_start_cursor = repo['cdeng.stations_popular_start'].find()
		# station_end_cursor = repo['cdeng.stations_popular_end'].find()

		# data for analysis
		data_dock_incoming_pair = []
		data_dock_outgoing_pair = []
		data_outgoing_incoming_pair = []

		for station_start in station_start_cursor:

			station_id = station_start.get('_id')
			start_dock = station_start.get('dock')
			start_trip = station_start.get('outgoing_trip')
			data_dock_outgoing_pair.append((start_dock, start_trip))

			station_end = list(repo['cdeng.stations_popular_end'].find({'_id': station_id}))[0]
			end_dock = station_end.get('dock')
			end_trip = station_end.get('incoming_trip')

			assert(start_dock == end_dock)

			data_dock_incoming_pair.append((end_dock, end_trip))

			data_outgoing_incoming_pair.append((start_trip, end_trip))

		# Stats analysis
		print('Calculating statistical results...')
		# 1. bike stations' # of docks and incoming trip
		# (1). Correlation coefficient (x: dock, y: incoming)
		x = [xi for (xi, yi) in data_dock_incoming_pair]
		y = [yi for (xi, yi) in data_dock_incoming_pair]
		dock_end_corr = helper.corr(x, y)

		# (2). P values
		dock_end_p_value = helper.p(x, y, permutation_size)

		inputDic_dock_incoming = {}
		inputDic_dock_incoming['relationship_name'] = 'x: dock, y: incoming'
		inputDic_dock_incoming['corr'] = dock_end_corr
		inputDic_dock_incoming['p_value'] = dock_end_p_value


		# 2. bike stations' # of docks and outgoing trip,
		# (1). Correlation coefficient (x: dock, y: outgoing)
		x = [xi for (xi, yi) in data_dock_outgoing_pair]
		y = [yi for (xi, yi) in data_dock_outgoing_pair]
		dock_start_corr = helper.corr(x, y)

		# (2). P values
		dock_start_p_value = helper.p(x, y, permutation_size)

		inputDic_dock_outgoing = {}
		inputDic_dock_outgoing['relationship_name'] = 'x: dock, y: outgoing'
		inputDic_dock_outgoing['corr'] = dock_start_corr
		inputDic_dock_outgoing['p_value'] = dock_start_p_value

		# 3. outgoing trip and incoming trip
		# (1). Correlation coefficient (x: outgoing, y: incoming)
		x = [xi for (xi, yi) in data_outgoing_incoming_pair]
		y = [yi for (xi, yi) in data_outgoing_incoming_pair]
		start_end_corr = helper.corr(x, y)

		# (2). P values
		start_end_p_value = helper.p(x, y, permutation_size)

		inputDic_outgoing_incoming = {}
		inputDic_outgoing_incoming['relationship_name'] = 'x: outgoing, y: incoming'
		inputDic_outgoing_incoming['corr'] = start_end_corr
		inputDic_outgoing_incoming['p_value'] = start_end_p_value

		print('Calculation finished. Storing results into a collection in MongoDB...')
		# Store results in a db
		# print out the result
		print('---Results---')
		print('1. Bike stations # of docks and incoming trip (x: dock, y: incoming):')
		print('Correlation coefficient: %s'%dock_end_corr)
		print('P values: %s'%dock_end_p_value)
		print()

		print('2. Bike stations # of docks and outgoing trip (x: dock, y: outgoing):')
		print('Correlation coefficient: %s'%dock_start_corr)
		print('P values: %s'%dock_start_p_value)	
		print()

		print('3. Outgoing trip and incoming trip (x: outgoing, y: incoming):')
		print('Correlation coefficient: %s'%start_end_corr)
		print('P values: %s'%start_end_p_value)	
		print()

		repo.dropCollection('cdeng.stations_dock_incoming_outgoing_stats')
		repo.createCollection('cdeng.stations_dock_incoming_outgoing_stats')

		repo['cdeng.stations_dock_incoming_outgoing_stats'].insert(inputDic_dock_outgoing)
		repo['cdeng.stations_dock_incoming_outgoing_stats'].insert(inputDic_dock_incoming)
		repo['cdeng.stations_dock_incoming_outgoing_stats'].insert(inputDic_outgoing_incoming)
		################################### End Operations here ###################################
		repo.logout()
		endTime = datetime.datetime.now()
		return {"start": startTime, "end": endTime}

	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
		Create the provenance document describing everything happening
		in this script. Each run of the script will generate a new
		document describing that invocation event.
		'''

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('cdeng', 'cdeng')

		################################### Finish data provenance here
		print("Finish data provenance here...")

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') 
		doc.add_namespace('dat', 'http://datamechanics.io/data/')
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
		doc.add_namespace('log', 'http://datamechanics.io/log/') 
		doc.add_namespace('bdp', 'http://datamechanics.io/data/')
		doc.add_namespace('bdp2', 'https://s3.amazonaws.com/hubway-data')

		this_script = doc.agent('alg:cdeng#Project2_data_analysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource1 = doc.entity('bdp:201801_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		resource2 = doc.entity('bdp:201802_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		resource3 = doc.entity('bdp2:Hubway_Stations_as_of_July_2017', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

		get_corr_dock_outgoing = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_pval_dock_outgoing = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		get_corr_dock_incoming = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_pval_dock_incoming = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		get_corr_outgoing_incoming = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_pval_outgoing_incoming = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		get_stations_dock_incoming_outgoing_stats = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_corr_dock_outgoing, this_script)
		doc.wasAssociatedWith(get_pval_dock_outgoing, this_script)

		doc.wasAssociatedWith(get_corr_dock_incoming, this_script)
		doc.wasAssociatedWith(get_pval_dock_incoming, this_script)

		doc.wasAssociatedWith(get_corr_outgoing_incoming, this_script)
		doc.wasAssociatedWith(get_pval_outgoing_incoming, this_script)

		doc.wasAssociatedWith(get_stations_dock_incoming_outgoing_stats, this_script)

		# get_corr_dock_outgoing
		doc.usage(get_corr_dock_outgoing, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_corr_dock_outgoing, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_corr_dock_outgoing, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		# get_pval_dock_outgoing
		doc.usage(get_pval_dock_outgoing, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_pval_dock_outgoing, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_pval_dock_outgoing, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		# get_corr_dock_incoming
		doc.usage(get_corr_dock_incoming, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_corr_dock_incoming, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_corr_dock_incoming, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		# get_pval_dock_incoming
		doc.usage(get_pval_dock_incoming, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_pval_dock_incoming, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_pval_dock_incoming, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		# get_corr_outgoing_incoming
		doc.usage(get_corr_outgoing_incoming, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_corr_outgoing_incoming, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_corr_outgoing_incoming, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		# get_pval_outgoing_incoming
		doc.usage(get_pval_outgoing_incoming, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_pval_outgoing_incoming, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_pval_outgoing_incoming, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		# get_stations_dock_incoming_outgoing_stats
		doc.usage(get_stations_dock_incoming_outgoing_stats, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_stations_dock_incoming_outgoing_stats, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_stations_dock_incoming_outgoing_stats, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		stations_dock_incoming_outgoing_stats = doc.entity('dat:cdeng#stations_dock_incoming_outgoing_stats', {prov.model.PROV_LABEL:'stations_dock_incoming_outgoing_stats', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(stations_dock_incoming_outgoing_stats, this_script)
		doc.wasGeneratedBy(stations_dock_incoming_outgoing_stats, get_stations_dock_incoming_outgoing_stats, endTime)

		doc.wasDerivedFrom(stations_dock_incoming_outgoing_stats, resource1, get_stations_dock_incoming_outgoing_stats, get_stations_dock_incoming_outgoing_stats, get_stations_dock_incoming_outgoing_stats)
		doc.wasDerivedFrom(stations_dock_incoming_outgoing_stats, resource2, get_stations_dock_incoming_outgoing_stats, get_stations_dock_incoming_outgoing_stats, get_stations_dock_incoming_outgoing_stats)
		doc.wasDerivedFrom(stations_dock_incoming_outgoing_stats, resource3, get_stations_dock_incoming_outgoing_stats, get_stations_dock_incoming_outgoing_stats, get_stations_dock_incoming_outgoing_stats)
		###################################
		repo.logout()
		return doc

class Data_analysis_helper:
	# Permutation
	def permute(self, x):
		shuffled = [xi for xi in x]
		shuffle(shuffled)
		return shuffled

	# Average
	def avg(self, x): 
		return sum(x)/len(x)

	# Standard deviation
	def stddev(self, x): 
		m = self.avg(x)
		return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

	# Covariance
	def cov(self, x, y): 
		return sum([(xi-self.avg(x))*(yi-self.avg(y)) for (xi,yi) in zip(x,y)])/len(x)

	# Correlation coefficient.
	def corr(self, x, y): 
		if self.stddev(x) * self.stddev(y) != 0:
			return self.cov(x, y)/(self.stddev(x) * self.stddev(y))

	# P-value
	def p(self, x, y, permutation_size=5000):
		c0 = self.corr(x, y)
		corrs = []
		for k in range(0, permutation_size):
			y_permuted = self.permute(y)
			corrs.append(self.corr(x, y_permuted))
		return len([c for c in corrs if abs(c) >= abs(c0)])/len(corrs)

# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
if __name__ == '__main__':
	print('####################Begin Calculation####################')
	Project2_data_analysis.execute(trial = True)
	doc = Project2_data_analysis.provenance()
	print(doc.get_provn())
	print(json.dumps(json.loads(doc.serialize()), indent=4))
	print('####################End Calculation####################')
## eof

