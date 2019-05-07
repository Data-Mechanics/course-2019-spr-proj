## This python file is under cdeng file

import json
import dml
import prov.model
import datetime
import uuid

import numpy as np
from math import *
# from Project2_Policy_Iteration_Bike_Model import Policy_Iteration_Bike_Sharing_Model


class Project2_optimal_allocation(dml.Algorithm):
	contributor = 'cdeng'
	reads = ['cdeng.stations_popular_start', 'cdeng.stations_popular_end']
	writes = ['cdeng.bike_allocation_strategy']

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
		print('Algorithm 3: Optimal allocation for the most 2 popular stations for project 2')
		print('NOTICE: the trial mode will take around 2-3 mins to run while the normal mode will take around 4.5 hrs to run.')

		if trial:
			print('Trial mode...')

		# Get the average of the each station's in/out rate
		station_start = list(repo['cdeng.stations_popular_start'].find().limit(2))
		station_end = list(repo['cdeng.stations_popular_end'].find().limit(2))

		# print(station_start)
		# print(station_end)

		reward = 2
		cost = 1
		discount_factor = 0.9
		move_bike_num = 5
		possion_limit = 14

		assert(station_start[0].get('_id') == station_end[0].get('_id'))
		assert(station_start[1].get('_id') == station_end[1].get('_id'))

		station_1_name = station_start[0].get('_id')
		station_2_name = station_start[1].get('_id')

		station_1_dock_num = station_start[0].get('dock')
		station_2_dock_num = station_start[1].get('dock')

		station_1_incoming_rate = station_end[0].get('avg_incomming_trip')
		station_1_outgoing_rate = station_start[0].get('avg_outgoing_trip')
		station_2_incoming_rate = station_end[1].get('avg_incomming_trip')
		station_2_outgoing_rate = station_start[1].get('avg_outgoing_trip')

		print('Here is the parameters for the policy iteration optimization algorithm:')
		print('reward: %s'%reward)
		print('cost: %s'%cost)
		print('discount_factor: %s'%discount_factor)
		print('move_bike_num: %s'%move_bike_num)
		print('possion_limit: %s'%possion_limit)
		print('station_1_dock_num: %s'%station_1_dock_num)
		print('station_2_dock_num: %s'%station_2_dock_num)

		print('station_1_incoming_rate: %s'%station_1_incoming_rate)
		print('station_1_outgoing_rate: %s'%station_1_outgoing_rate)
		print('station_2_incoming_rate: %s'%station_2_incoming_rate)
		print('station_2_outgoing_rate: %s'%station_2_outgoing_rate)

		print('-------------------------------------------------')
		print('Algorithm begins...')


		if trial:
			# Model for trial mode
			model_trial = Policy_Iteration_Bike_Sharing_Model(reward, cost, discount_factor,
				move_bike_num, possion_limit,station_1_dock_num, station_2_dock_num,
				station_1_incoming_rate, station_1_outgoing_rate, 
				station_2_incoming_rate, station_2_outgoing_rate)
			policy_grid_trial, state_value_grid_trial = model_trial.policy_iteration(trial=True)
			# Need to do this after the policy iteration!!!!!
			model_trial.update_policy_value(policy_grid_trial, state_value_grid_trial)

			# Store result to MongoDB
			repo.dropCollection("cdeng.bike_allocation_strategy")
			repo.createCollection("cdeng.bike_allocation_strategy")
			inputDic = {}
			inputDic['station_1_name'] = station_1_name
			inputDic['station_2_name'] = station_2_name
			inputDic['reward'] = reward
			inputDic['cost'] = cost
			inputDic['discount_factor'] = discount_factor
			inputDic['move_bike_num'] = move_bike_num
			inputDic['possion_limit'] = possion_limit
			inputDic['station_1_dock_num'] = station_1_dock_num
			inputDic['station_2_dock_num'] = station_2_dock_num
			inputDic['station_1_incoming_rate'] = station_1_incoming_rate
			inputDic['station_1_outgoing_rate'] = station_1_outgoing_rate
			inputDic['station_2_incoming_rate'] = station_2_incoming_rate
			inputDic['station_2_outgoing_rate'] = station_2_outgoing_rate
			inputDic['strategy'] = policy_grid_trial.tolist()
			inputDic['state_value'] = state_value_grid_trial.tolist()
			repo['cdeng.bike_allocation_strategy'].insert(inputDic)

		else:
			# Model for full mode
			model = Policy_Iteration_Bike_Sharing_Model(reward, cost, discount_factor,
				move_bike_num, possion_limit,station_1_dock_num, station_2_dock_num,
				station_1_incoming_rate, station_1_outgoing_rate, 
				station_2_incoming_rate, station_2_outgoing_rate)

			policy_grid, state_value_grid = model.policy_iteration()
			# Need to do this after the policy iteration!!!!!
			model.update_policy_value(policy_grid, state_value_grid)

			# Store result to MongoDB
			repo.dropCollection("cdeng.bike_allocation_strategy")
			repo.createCollection("cdeng.bike_allocation_strategy")
			inputDic = {}
			inputDic['station_1_name'] = station_1_name
			inputDic['station_2_name'] = station_2_name
			inputDic['reward'] = reward
			inputDic['cost'] = cost
			inputDic['discount_factor'] = discount_factor
			inputDic['move_bike_num'] = move_bike_num
			inputDic['possion_limit'] = possion_limit
			inputDic['station_1_dock_num'] = station_1_dock_num
			inputDic['station_2_dock_num'] = station_2_dock_num
			inputDic['station_1_incoming_rate'] = station_1_incoming_rate
			inputDic['station_1_outgoing_rate'] = station_1_outgoing_rate
			inputDic['station_2_incoming_rate'] = station_2_incoming_rate
			inputDic['station_2_outgoing_rate'] = station_2_outgoing_rate
			inputDic['strategy'] = policy_grid.tolist()
			inputDic['state_value'] = state_value_grid.tolist()
			repo['cdeng.bike_allocation_strategy'].insert(inputDic)


		print('Store output polict strategy and values into MongoDB...')
		print('Done!')
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

		this_script = doc.agent('alg:cdeng#Project2_optimal_allocation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource1 = doc.entity('bdp:201801_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		resource2 = doc.entity('bdp:201802_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		resource3 = doc.entity('bdp2:Hubway_Stations_as_of_July_2017', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

		get_most_2_incoming_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_most_2_outgoing_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_allocation_strategies = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_most_2_incoming_stations, this_script)
		doc.wasAssociatedWith(get_most_2_outgoing_stations, this_script)
		doc.wasAssociatedWith(get_allocation_strategies, this_script)

		# get_most_2_outgoing_stations
		doc.usage(get_most_2_outgoing_stations, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_most_2_outgoing_stations, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_most_2_outgoing_stations, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		# get_most_2_incoming_stations
		doc.usage(get_most_2_incoming_stations, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_most_2_incoming_stations, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_most_2_incoming_stations, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		# get_allocation_strategies
		doc.usage(get_allocation_strategies, resource1, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_allocation_strategies, resource2, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		doc.usage(get_allocation_strategies, resource3, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':''
			}
			)

		allocation_strategies = doc.entity('dat:cdeng#allocation_strategies', {prov.model.PROV_LABEL:'allocation_strategies', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(allocation_strategies, this_script)
		doc.wasGeneratedBy(allocation_strategies, get_allocation_strategies, endTime)

		doc.wasDerivedFrom(allocation_strategies, resource1, get_allocation_strategies, get_allocation_strategies, get_allocation_strategies)
		doc.wasDerivedFrom(allocation_strategies, resource2, get_allocation_strategies, get_allocation_strategies, get_allocation_strategies)
		doc.wasDerivedFrom(allocation_strategies, resource3, get_allocation_strategies, get_allocation_strategies, get_allocation_strategies)


		###################################
		repo.logout()
		return doc


# Class for the optimization problem
class Policy_Iteration_Bike_Sharing_Model:
	def __init__(self, reward, cost, discount_factor,
		move_bike_num, possion_limit,
		station_1_dock_num, station_2_dock_num, 
		station_1_incoming_rate, station_1_outgoing_rate, 
		station_2_incoming_rate, station_2_outgoing_rate):

		# Hyper-parameters
		self.reward = reward
		self.cost = cost
		self.move_bike_num = move_bike_num

		self.station_1_dock_num = station_1_dock_num
		self.station_2_dock_num = station_2_dock_num

		self.station_1_incoming_rate = station_1_incoming_rate
		self.station_1_outgoing_rate = station_1_outgoing_rate

		self.station_2_incoming_rate = station_2_incoming_rate
		self.station_2_outgoing_rate = station_2_outgoing_rate

		self.lambda_term_cache = dict()
		self.discount_factor = discount_factor
		self.poisson_limit = possion_limit

		# MDP properties      
		self.actions = np.arange(-self.move_bike_num, self.move_bike_num+1)
		self.policy_grid_storer = np.zeros((self.station_1_dock_num+1, self.station_2_dock_num+1))
		self.state_value_grid_storer = np.zeros((self.station_1_dock_num+1, self.station_2_dock_num+1))

	def poisson_density(self, n, lambda_term):
		key = str(n) + '_' +str(lambda_term)
		# Store the lambda term for future use
		if key not in self.lambda_term_cache.keys():
			self.lambda_term_cache[key] = exp(-lambda_term) * pow(lambda_term, n)/factorial(n)
		return self.lambda_term_cache.get(key)


	# This method is for the policy evaluation
	def return_expectation(self, state, action, state_value, trial=False):
		# Initialize return (minus the cost of the action)
		return_value = 0.0
		# Deduct the cost first
		return_value -= self.cost * abs(action)

		# Loop through all possible outgoing bikes situations for both stations
		for station_1_outgoing_num in range(0, self.poisson_limit):
			for station_2_outgoing_num in range(0, self.poisson_limit):

				# Get the number of bikes after moving the bikes
				station_1_bike_num = int(min(state[0] - action, self.station_1_dock_num))
				station_2_bike_num = int(min(state[1] + action, self.station_2_dock_num))

				# Get the actual outgoing rental 
				station_1_outgoing_num_actual = int(min(station_1_bike_num, station_1_outgoing_num))
				station_2_outgoing_num_actual = int(min(station_2_bike_num, station_1_outgoing_num))

				# Calculate the reward for outgoing bikes (successful rental)
				outgoing_bike_reward = self.reward * (station_1_outgoing_num_actual + station_2_outgoing_num_actual)

				# Exclude those bikes that have already outgone
				station_1_bike_num -= station_1_outgoing_num_actual
				station_2_bike_num -= station_2_outgoing_num_actual

				# Obtain the probability that station 1, 2 will have the outgoing bikes above
				station_1_outgoing_prob = self.poisson_density(station_1_outgoing_num, self.station_1_outgoing_rate)
				station_2_outgoing_prob = self.poisson_density(station_2_outgoing_num, self.station_2_outgoing_rate)
				outgoing_prob = station_1_outgoing_prob * station_2_outgoing_prob

				if not trial:
					# Loop through every possible incoming bikes situations for both stations
					for station_1_incoming_num in range(0, self.poisson_limit):
						for station_2_incoming_num in range(0, self.poisson_limit):

							# Update the number of bikes in each station
							station_1_bike_num_next = int(min(station_1_bike_num + station_1_incoming_num, self.station_1_dock_num))
							station_2_bike_num_next = int(min(station_2_bike_num + station_1_incoming_num, self.station_2_dock_num))

							# Obtain the probability that station 1, 2 will have the incoming bikes above
							station_1_incoming_prob = self.poisson_density(station_1_incoming_num, self.station_1_incoming_rate)
							station_2_incoming_prob = self.poisson_density(station_2_incoming_num, self.station_2_incoming_rate)
							incoming_prob = station_1_incoming_prob * station_2_incoming_prob
							total_prob = incoming_prob * outgoing_prob

							next_state_value = state_value[station_1_bike_num_next, station_2_bike_num_next]
							return_value += total_prob * (outgoing_bike_reward + self.discount_factor * next_state_value)
				else:
					# Use constant incoming rate for less loops (trial mode)
					# Update the number of bikes in each station
					station_1_incoming_num = self.station_1_incoming_rate
					station_2_incoming_num = self.station_2_incoming_rate

					station_1_bike_num_next = int(min(station_1_bike_num + station_1_incoming_num, self.station_1_dock_num))
					station_2_bike_num_next = int(min(station_2_bike_num + station_1_incoming_num, self.station_2_dock_num))
					total_prob = outgoing_prob
					next_state_value = state_value[station_1_bike_num_next, station_2_bike_num_next]
					return_value += total_prob * (outgoing_bike_reward + self.discount_factor * next_state_value)

		return return_value

	def update_policy_value(self, policy, value):
		self.policy_grid_storer = policy
		self.state_value_grid_storer = value

	def policy_iteration(self, epsilon=1e-4, trial=False):

		policy_grid = np.copy(self.policy_grid_storer)
		state_value_grid = np.copy(self.state_value_grid_storer)
		total_steps = 0

		while True:
			# Policy evaluation
			evaluation_steps = 0

			while True:
				state_value_grid_updated = np.copy(state_value_grid)

				for i in range(self.station_1_dock_num + 1):
					for j in range(self.station_2_dock_num + 1):
						state_value_grid_updated[i, j] = self.return_expectation([i, j], policy_grid[i, j], state_value_grid_updated, trial)

				state_value_delta = np.sum(np.abs(state_value_grid_updated - state_value_grid))
				state_value_grid = state_value_grid_updated

				if evaluation_steps % 2 == 0:
					print('Evaluation step: %s, change: %s'%(evaluation_steps, state_value_delta))

				# The state values are converged and break the policy evaluation loop
				if state_value_delta < epsilon:
					print('Evaluation step: %s, change: %s - evaluation finished'%(evaluation_steps, state_value_delta))
					break

				evaluation_steps += 1
				
			# Policy improvement
			improvement_steps = 0
			policy_grid_updated = np.copy(policy_grid)
			for i in range(self.station_1_dock_num + 1):
				for j in range(self.station_2_dock_num + 1):
					action_returns = []
					for action in self.actions:
						if (i >= action and action >= 0) or (j >= abs(action) and action < 0):
							return_value = self.return_expectation([i, j], action, state_value_grid, trial)
							action_returns.append(return_value)
						else:
							action_returns.append(float('-inf'))
					# Obtain the greedy policty
					policy_grid_updated[i, j] = self.actions[np.argmax(action_returns)]

			policy_delta = np.sum(policy_grid_updated != policy_grid)
			policy_grid = policy_grid_updated

			print('Policy changed at total steps %s! Policy improvement finished!'%(total_steps))

			if policy_delta == 0:
				print('Policy converged at total steps %s! Policy iteration finished!'%(total_steps))
				break
			total_steps += 1

		return policy_grid, state_value_grid

# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
if __name__ == '__main__':
	print('####################Begin Calculation####################')
	Project2_optimal_allocation.execute(trial = True)
	doc = Project2_optimal_allocation.provenance()
	print(doc.get_provn())
	print(json.dumps(json.loads(doc.serialize()), indent=4))
	print('####################End Calculation####################')
## eof