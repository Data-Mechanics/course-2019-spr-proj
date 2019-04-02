import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import requests
import csv
from tqdm import tqdm
import numpy as np
from sklearn.cluster import KMeans

class Kmeans(dml.Algorithm):
	contributor = 'charr_hu38_npearce'
	reads = ['charr_hu38_npearce.boston_s', 'charr_hu38_npearce.washington_s', 'charr_hu38_npearce.newyork_s', 'charr_hu38_npearce.chicago_s', 'charr_hu38_npearce.sanfran_s']
	writes = ['charr_hu38_npearce.Kmeans']
	
	@staticmethod
	def execute(trial = False):
		'''Union dataset containing bike data into the dataset containing city census information'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('charr_hu38_npearce', 'charr_hu38_npearce')

		repo.dropCollection("Kmeans")
		repo.createCollection("Kmeans")
		
		boston_s = list(repo.charr_hu38_npearce.boston_s.find())
		
		if(not trial):		#Restrict trial data to Boston Data only
			washington_s = list(repo.charr_hu38_npearce.washington_s.find())
			newyork_s = list(repo.charr_hu38_npearce.newyork_s.find())
			chicago_s = list(repo.charr_hu38_npearce.chicago_s.find())
			sanfran_s = list(repo.charr_hu38_npearce.sanfran_s.find())
		
		#K-means Code
		k=5
		P=[]
		P.append([(boston_s[i]['lat'],boston_s[i]['lon']) for i in range(len(boston_s))] )
		
		if(not trial):		#Restrict trial data to Boston Data only
			P.append([(washington_s[i]['lat'],washington_s[i]['lon']) for i in range(len(washington_s))] )
			P.append([(newyork_s[i]['lat'],newyork_s[i]['lon']) for i in range(len(newyork_s))])
			P.append([(chicago_s[i]['lat'],chicago_s[i]['lon']) for i in range(len(chicago_s))])
			P.append([(sanfran_s[i]['lat'],sanfran_s[i]['lon']) for i in range(len(sanfran_s))])

		cities=["Boston", "Washington", "New York", "Chicago", "San Francisco"]
		data_arry=[]
		x = 1 if trial else 5	#Restrict trial data to Boston Data only
		for i in range(x):
			kmeans = KMeans(n_clusters=k, random_state=0).fit(P[i])
			temp=np.ndarray.tolist(kmeans.cluster_centers_)
			data_arry.append({"city":cities[i],"locs":temp})	
		#print(finalM[0])
		
		
		repo['charr_hu38_npearce.Kmeans'].insert_many(data_arry)							
		repo['charr_hu38_npearce.Kmeans'].metadata({'complete':True})

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
		repo.authenticate('charr_hu38_npearce', 'charr_hu38_npearce')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:charr_hu38_npearce#Kmeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Union of Bike and Census Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		
		get_Kmeans = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_Kmeans, this_script)
		doc.usage(get_Kmeans, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'
				  }
				  )

		Kmeans = doc.entity('dat:charr_hu38_npearce#Kmeans', {prov.model.PROV_LABEL:'Optimal station locations for each city', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(Kmeans, this_script)
		doc.wasGeneratedBy(Kmeans, get_Kmeans, endTime)
		doc.wasDerivedFrom(Kmeans, resource, get_Kmeans, get_Kmeans, get_Kmeans)
		
		repo.logout()
				  
		return doc
		

# This is Kmeans code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
Kmeans.execute()
#doc = Kmeans.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof