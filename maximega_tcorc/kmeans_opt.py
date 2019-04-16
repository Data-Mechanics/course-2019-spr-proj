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
#from maximega_tcorc.helper_functions.cons_sat import cons_sat
from helper_functions.cons_sat import cons_sat
#from maximega_tcorc.helper_functions.lat_long_kmeans import run_lat_long_kmeans
from helper_functions.lat_long_kmeans import run_lat_long_kmeans


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

		#------------------ K Means -----------------
		kmeans = run_lat_long_kmeans(X)

		k = 5
		k_groupings = kmeans.labels_

		for i in range(len(data_copy)):
			data_copy[i]['zone'] = k_groupings[i]
		avg_inc = [0] * k
		count_inc = [0] * k
		for item in data_copy:
			avg_inc[item['zone']] += item['income']
			count_inc[item['zone']] += 1
		for i in range(len(avg_inc)):
			avg_inc[i] /= count_inc[i]

		for i in range(len(data_copy)):
			data_copy[i]['avg_inc'] = avg_inc[data_copy[i]['zone']]

		for i in range(k):
			min_avg = min(avg_inc)
			for item in data_copy:
				if (item['avg_inc'] == min_avg):
					item['zone'] = i
			avg_inc.remove(min_avg)
			
		x = cons_sat(data_copy, k)
		print(x.translate)
		for i in x:
			print(i.params)
		
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
kmeans_opt.execute()