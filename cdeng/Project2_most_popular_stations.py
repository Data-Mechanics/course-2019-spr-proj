## This python file is under cdeng file

import json
import dml
import prov.model
import datetime
from bson.son import SON
import uuid


class Project2_most_popular_stations(dml.Algorithm):
	contributor = 'cdeng'
	reads = ['cdeng.bike_trip', 'cdeng.stations_info']
	writes = ['cdeng.stations_popular_start', 'cdeng.stations_popular_end']

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
		print('Algorithm 2: find popular stations for project 2')

		# Calculate the average rate perday
		hour_num_Jan, hour_num_Feb = 31 * 9, 28 * 9
		time_num = hour_num_Jan + hour_num_Feb

		
		# #1 Find the stations high really high outgoing trip
		repo.dropCollection('cdeng.stations_popular_start')
		repo.createCollection('cdeng.stations_popular_start')

		pipeline = [
			{'$lookup': {
				'from' : 'cdeng.bike_trip',
				'localField' : 'Station',
				'foreignField' : 'start station name',
				'as' : 'start_trip'
				}
			},

			{'$project': {
					'_id': '$Station',
					'outgoing_trip': {'$size': '$start_trip'},
					'dock': '$# of Docks',
					'avg_outgoing_trip': {'$toInt': {'$divide': [{'$size': '$start_trip'}, time_num]}},
					'Coordinate': {'Lat': '$Latitude', 'Long': '$Longitude'}
				}
			},

			{'$sort': SON([("outgoing_trip", -1), ("dock", -1)])},
			{ '$out' : 'cdeng.stations_popular_start' }
		]

		repo['cdeng.stations_info'].aggregate(pipeline)


		# #2 Find the stations high really high incoming trip
		repo.dropCollection('cdeng.stations_popular_end')
		repo.createCollection('cdeng.stations_popular_end')

		pipeline2 = [
			{'$lookup': {
				'from' : 'cdeng.bike_trip',
				'localField' : 'Station',
				'foreignField' : 'end station name',
				'as' : 'end_trip'
				}
			},

			{'$project': {
					'_id': '$Station',
					'incoming_trip': {'$size': '$end_trip'},
					'dock': '$# of Docks',
					'avg_incomming_trip': {'$toInt': {'$divide': [{'$size': '$end_trip'}, time_num]}},
					'Coordinate': {'Lat': '$Latitude', 'Long': '$Longitude'}
				}
			},

			{'$sort': SON([("incoming_trip", -1), ("dock", -1)])},
			{ '$out' : 'cdeng.stations_popular_end' }
		]

		repo['cdeng.stations_info'].aggregate(pipeline2)
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

		resource1 = doc.entity('bdp:201801_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		resource2 = doc.entity('bdp:201802_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		resource3 = doc.entity('bdp2:Hubway_Stations_as_of_July_2017', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

		get_most_outcoming = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_most_incoming = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		this_script = doc.agent('alg:cdeng#Project2_most_popular_stations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		doc.wasAssociatedWith(get_most_outcoming, this_script)
		doc.wasAssociatedWith(get_most_incoming, this_script)

		doc.usage(get_most_outcoming, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_most_outcoming, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_most_outcoming, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_most_incoming, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_most_incoming, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_most_incoming, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		stations_popular_start = doc.entity('dat:cdeng#stations_popular_start', {prov.model.PROV_LABEL:'stations_popular_start', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(stations_popular_start, this_script)
		doc.wasGeneratedBy(stations_popular_start, get_most_outcoming, endTime)
		doc.wasDerivedFrom(stations_popular_start, resource1, get_most_outcoming, get_most_outcoming, get_most_outcoming)
		doc.wasDerivedFrom(stations_popular_start, resource2, get_most_outcoming, get_most_outcoming, get_most_outcoming)
		doc.wasDerivedFrom(stations_popular_start, resource3, get_most_outcoming, get_most_outcoming, get_most_outcoming)

		stations_popular_end = doc.entity('dat:cdeng#stations_popular_end', {prov.model.PROV_LABEL:'stations_popular_end', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(stations_popular_end, this_script)
		doc.wasGeneratedBy(stations_popular_end, get_most_incoming, endTime)
		doc.wasDerivedFrom(stations_popular_end, resource1, get_most_incoming, get_most_incoming, get_most_incoming)
		doc.wasDerivedFrom(stations_popular_end, resource2, get_most_incoming, get_most_incoming, get_most_incoming)
		doc.wasDerivedFrom(stations_popular_end, resource3, get_most_incoming, get_most_incoming, get_most_incoming)

		###################################
		repo.logout()
		return doc

# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
if __name__ == '__main__':
	print('####################Begin Calculation####################')
	Project2_most_popular_stations.execute(trial=True)
	doc = Project2_most_popular_stations.provenance()
	print(doc.get_provn())
	print(json.dumps(json.loads(doc.serialize()), indent=4))
	print('####################End Calculation####################')
## eof