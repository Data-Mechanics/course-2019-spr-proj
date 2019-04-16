import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
import pandas
from pandas.plotting import parallel_coordinates
from maximega_tcorc.helper_functions.lat_long_kmeans import run_lat_long_kmeans


class kmeans_opt(dml.Algorithm):
	contributor = 'maximega_tcorc'
	reads = ['maximega_tcorc.income_with_NTA_with_percentages']
	writes = []
	
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		#repo_name = kmeans_opt.writes[0]
		# ----------------- Set up the database connection -----------------
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('maximega_tcorc', 'maximega_tcorc')

		nta_objects = repo.maximega_tcorc.income_with_NTA_with_percentages.find()

		if trial:
			nta_objects = nta_objects[0:50]
		
		X = []
		data_copy = []
		for nta in nta_objects:
			if(len(nta['stations'])!= 0):
				income = nta['income']
				X.append([nta['ntaname'], nta['position'][0], nta['position'][1], income])
				data_copy.append(nta)

		run_lat_long_kmeans(X)





		
		# #------------------ K Means
		# k = 5
		# kmeans = KMeans(n_clusters=k, verbose=0, n_init = 100).fit(X)
		# kmeans.fit_predict(X)
		# #X = np.array(X)

		# k_groupings = kmeans.labels_

		# for i in range(len(data_copy)):
		# 	data_copy[i]['zone'] = k_groupings[i]

		
		# totals_real = [0] * k
		# for item in data_copy:
		# 	totals_real[item['zone']] += (float(item['population']) * (item['trans_percent'] / 100)) * 2.75

		# overall_total_real = sum(totals_real)

		# avgs_real = [0] * k
		# for i in range(len(totals_real)):
		# 	avgs_real[i] = totals_real[i]/overall_total_real
		# #print(avgs_real)
		

		# #plt.scatter(X[:, 0], X[:, 1], s=30, c=kmeans.labels_)
		# #plt.show()

		# data = []
		# for i in range(len(X)):
		# 	s = str(k_groupings[i])
		# 	data.append({'Latitude': X[i][0], 'Longitute': X[i][1], 'Income': X[i][2], 'Population': X[i][3], 'Zone' : s})
		
		# parallel_coordinates(data, 'Zone')
		# plt.show()

		# hypothetical_percentages = [0.14, 0.42, 0.08, 0.18, 0.18] #ik its a shit name we'll figure it out


		# # equation: (percentage income for each "zone") = (populaiton * public_transport_%) * X 
		# # (percentage income for each "zone") / (populaiton * public_transport_%) = X
		# # it doesnt really work but this is what we had on paper
		# totals_projected = [0] * k
		# for item in data_copy:
		# 	totals_projected[item['zone']] += float(item['population']) * (item['trans_percent'] / 100)
		
		# new_zone_fares = [0] * k
		# for i in range(len(new_zone_fares)):
		# 	new_zone_fares[i] = (hypothetical_percentages[i] * overall_total_real) / totals_projected[i]






		
		#print(new_zone_fares)

		# ----------------- Error

		# error = np.zeros(25)
		# for k in range(1,25):
		# 	kmeans = KMeans(init='k-means++', n_clusters=k, n_init=100)
		# 	kmeans.fit(X)
		# 	error[k] = kmeans.inertia_

		# plt.scatter(range(1,len(error)),error[1:])
		# plt.xlabel('Number of clusters')
		# dummy = plt.ylabel('Error')
		# plt.show()
		
		# ----------------- Reformat data for mongodb insertion -----------------
		# insert_many_arr = []
		# for key in nta_objects.keys():
		# 	insert_many_arr.append(nta_objects[key])

		#----------------- Data insertion into Mongodb ------------------
		# repo.dropCollection('neighborhoods_with_stations')
		# repo.createCollection('neighborhoods_with_stations')
		# repo[repo_name].insert_many(insert_many_arr)
		# repo[repo_name].metadata({'complete':True})
		# print(repo[repo_name].metadata())

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
			Create the provenance document describing everything happening
			in this script. Each run of the script will generate a new
			document describing that invocation event.
		'''

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('maximega_tcorc', 'maximega_tcorc')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		#agent
		this_script = doc.agent('alg:maximega_tcorc#kmeans_opt', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		#resource
		nta_objects = doc.entity('dat:maximega_tcorc#nta_objects', {prov.model.PROV_LABEL:'Income with NTA with Percentages', prov.model.PROV_TYPE:'ont:DataSet'})
		#agent
		running_k_means = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(running_k_means, this_script)

		doc.usage(running_k_means, nta_objects, startTime, None,
					{prov.model.PROV_TYPE:'ont:Computation'
					}
					)
		#reasource
		categorized_ntas = doc.entity('dat:maximega_tcorc#categorized_ntas', {prov.model.PROV_LABEL:'Categorized NTAS', prov.model.PROV_TYPE:'ont:DataSet'})
		
		doc.wasAttributedTo(categorized_ntas, this_script)
		doc.wasGeneratedBy(categorized_ntas, running_k_means, endTime)
		doc.wasDerivedFrom(categorized_ntas, nta_objects, running_k_means, running_k_means, running_k_means)

		repo.logout()
				
		return doc

# kmeans_opt.execute()