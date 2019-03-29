import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt

class kmeans_opt(dml.Algorithm):
	contributor = 'maximega_tcorc'
	reads = ['maximega_tcorc.income_with_NTA_with_percentages']
	writes = []
	
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# repo_name = merge_stations_nta.writes[0]
		# ----------------- Set up the database connection -----------------
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('maximega_tcorc', 'maximega_tcorc')

		nta_objects = repo.maximega_tcorc.income_with_NTA_with_percentages.find()
		
		X = []
		names = []
		for nta in nta_objects:
			pop_times_perc = float(nta['population']) * (nta['trans_percent'] / 100)
			income = nta['income']

			X.append([income, pop_times_perc])
			names.append(nta['ntaname'])

		#------------------ K Means
		# k = 6
		# kmeans = KMeans(n_clusters=k).fit(X)
		# kmeans.fit_predict(X)

		# X = np.array(X)
		# plt.scatter(X[:, 0], X[:, 1], s=30, c=kmeans.labels_, edgecolors=['red', 'green', 'blue', 'red', 'green', 'blue'])
		# plt.show()



		# ----------------- Error

		# error = np.zeros(25)
		# for k in range(1,25):
		# 	kmeans = KMeans(init='k-means++', n_clusters=k, n_init=10)
		# 	kmeans.fit(X)
		# 	error[k] = kmeans.inertia_

		# plt.plot(range(1,len(error)),error[1:])
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