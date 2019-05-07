## This python file is under cdeng file

import json
import dml
import prov.model
import datetime
import numpy as np
import uuid
from flask import Flask, render_template

'''
This visualization code will create several web-based visualizations for this project
'''

class Project3_Visualizations(dml.Algorithm):
	contributor = 'cdeng'
	reads = ['cdeng.stations_info', 'cdeng.bike_allocation_strategy', 'cdeng.stations_popular_start', 'cdeng.stations_popular_end']
	writes = []

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
		visualization(repo)
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
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') 
		doc.add_namespace('dat', 'http://datamechanics.io/data/')
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
		doc.add_namespace('log', 'http://datamechanics.io/log/') 
		doc.add_namespace('bdp', 'http://datamechanics.io/data/')
		doc.add_namespace('bdp2', 'https://s3.amazonaws.com/hubway-data')

		resource1 = doc.entity('bdp:201801_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		resource2 = doc.entity('bdp:201802_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		resource3 = doc.entity('bdp2:Hubway_Stations_as_of_July_2017', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

		visualization = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		this_script = doc.agent('alg:cdeng#Project3_Visualizations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		doc.wasAssociatedWith(visualization, this_script)


		doc.usage(visualization, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(visualization, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(visualization, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		# Didn't create any new entities. So new entities initializations
		###################################
		repo.logout()
		return doc

'''
The following global funcions are for the visualization
'''

def prepare_3D_data(repo):
	optimization_result = repo['cdeng.bike_allocation_strategy'].find_one()

	# policy
	policy = optimization_result.get('strategy')
	x_axis_len = len(policy)
	y_axis_len = len(policy[0])

	x_value = []
	y_value = []
	z_value = []

	for i in range(x_axis_len):
		for j in range(y_axis_len):
			x_value.append(i)
			y_value.append(j)
			z_value.append(policy[i][j])

	# state value
	state_value = optimization_result.get('state_value')

	return {"policy_x": x_value, "policy_y": y_value, "policy_z": z_value, "state_value": state_value}


def prepare_station_data(repo):
	lat_list = []
	lon_list = []
	dock_num_list = []
	dock_num_text_list = []
	name_list = []


	bike_station_info = repo['cdeng.stations_info'].find()

	for station in bike_station_info:

		lat_list.append(station.get('Latitude'))
		lon_list.append(station.get('Longitude'))

		dock_num_list.append(station.get('# of Docks'))

		dock_num_text_list.append(str(station.get('# of Docks')))
		name_list.append(station.get('Station'))

		assert(len(lat_list) == len(lon_list))
		assert(len(lon_list) == len(dock_num_list))
		assert(len(dock_num_list) == len(name_list))

	return {'lat': lat_list, 'lon': lon_list, 'docks_size': dock_num_list}, {'dock_text': dock_num_text_list, 'name': name_list}

def prepare_incoming_data(repo):
	lat_list = []
	lon_list = []
	dock_num_list = []
	dock_num_text_list = []
	name_list = []

	bike_incoming_info = repo['cdeng.stations_popular_end'].find()

	for station in bike_incoming_info:

		lat_list.append(station.get('Coordinate').get('Lat'))
		lon_list.append(station.get('Coordinate').get('Long'))

		dock_num_list.append(station.get('dock'))

		dock_num_text_list.append(station.get('incoming_trip'))
		name_list.append(station.get('_id'))

		assert(len(lat_list) == len(lon_list))
		assert(len(lon_list) == len(dock_num_list))
		assert(len(dock_num_list) == len(name_list))

	return {'lat': lat_list, 'lon': lon_list, 'docks_size': dock_num_list}, {'incoming_text': dock_num_text_list, 'name': name_list}


def prepare_outgoing_data(repo):
	lat_list = []
	lon_list = []
	dock_num_list = []
	dock_num_text_list = []
	name_list = []

	bike_outgoing_info = repo['cdeng.stations_popular_start'].find()

	for station in bike_outgoing_info:
		lat_list.append(station.get('Coordinate').get('Lat'))
		lon_list.append(station.get('Coordinate').get('Long'))

		dock_num_list.append(station.get('dock'))

		dock_num_text_list.append(station.get('outgoing_trip'))
		name_list.append(station.get('_id'))

		assert(len(lat_list) == len(lon_list))
		assert(len(lon_list) == len(dock_num_list))
		assert(len(dock_num_list) == len(name_list))

	return {'lat': lat_list, 'lon': lon_list, 'docks_size': dock_num_list}, {'outgoing_trip': dock_num_text_list, 'name': name_list}


def prepare_popular_stations_chart_data(repo, top):
	outgoing_name = []
	outgoing_data = []

	incoming_name = []
	incoming_data = []

	# Through outgoing data
	bike_outgoing_info = repo['cdeng.stations_popular_start'].find()
	count = 1

	
	for station in bike_outgoing_info:
		if count > top:
			break

		outgoing_name.append(station.get('_id'))
		outgoing_data.append(station.get('outgoing_trip'))

		count += 1


	# Through incoming data
	bike_incoming_info = repo['cdeng.stations_popular_end'].find()
	count = 1

	for station in bike_incoming_info:
		if count > top:
			break

		incoming_name.append(station.get('_id'))
		incoming_data.append(station.get('incoming_trip'))

		count += 1


	return {'start_data': outgoing_data, 'end_data': incoming_data}, {'start_name': outgoing_name, 'end_name': incoming_name}








def visualization(repo):
	# Retrieve the data
	# Part 1: Data

	# (1). Map for station and dock #
	station_values, station_labels = prepare_station_data(repo)

	# (2). Map for station and incoming rate #
	incoming_values, incoming_labels = prepare_incoming_data(repo)

	# (3). Map for station and outgoing rate #
	outgoing_values, outgoing_labels = prepare_outgoing_data(repo)

	# (4). Chart for popular stations (top 5)
	chart_data, chart_label = prepare_popular_stations_chart_data(repo, 5)


	dataset_data = [station_values, incoming_values, outgoing_values]
	dataset_label = [station_labels, incoming_labels, outgoing_labels]

	# Part 2: Optimization
	policy_data = prepare_3D_data(repo)

	# Initialize Flask
	app = Flask(__name__);

	@app.route("/maps")
	def data_maps():
		return render_template('data_maps.html', values=dataset_data, labels=dataset_label)


	@app.route("/policy")
	def policy_3D():
		# data = {"x": [2, 9, 8, 5, 1], "y": [6, 1, 2, 4, 8], "z": [4, 11, 8, 15, 3]}
		return render_template('policy_and_state.html', values=policy_data)


	@app.route("/chart")
	def popular_station_chart():
		return render_template('chart.html', values=chart_data, labels=chart_label)

	app.run(debug=False)




# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
if __name__ == '__main__':
	print('####################Begin Calculation####################')
	Project3_Visualizations.execute()
	# doc = Project2_data_analysis.provenance()
	# print(doc.get_provn())
	# print(json.dumps(json.loads(doc.serialize()), indent=4))
	print('####################End Calculation####################')
## eof

