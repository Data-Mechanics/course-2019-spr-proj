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

class StationDataCollection(dml.Algorithm):
	contributor = 'charr_hu38_npearce'
	reads = []
	writes = ['charr_hu38_npearce.boston_s', 'charr_hu38_npearce.washington_s', 'charr_hu38_npearce.newyork_s', 'charr_hu38_npearce.chicago_s', 'charr_hu38_npearce.sanfran_s']

	@staticmethod
	def execute(trial = False, max_num=10):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('charr_hu38_npearce', 'charr_hu38_npearce')
		
		if(not repo['charr_hu38_npearce.sanfran_s'].count() == 0):
			repo.logout()

			endTime = datetime.datetime.now()
	
			return {"start":startTime, "end":endTime}
			
		url = 'https://gbfs.bluebikes.com/gbfs/en/station_information.json'													
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		r=r['data']['stations']
		data_arry=[]
		for i in tqdm(list(range(len(r)))):
			lat=r[i]['lat']
			lon=r[i]['lon']																							#Trivial Selection
			data_arry.append({"lat":lat,"lon":lon})																	#Trivial Projection
		repo.dropCollection("boston_s")
		repo.createCollection("boston_s")
		repo['charr_hu38_npearce.boston_s'].insert_many(data_arry)													#Data set 1: Boston
		repo['charr_hu38_npearce.boston_s'].metadata({'complete':True})
		
		if(trial):					#Restrict trial data to a single data set
			repo.logout()
			endTime = datetime.datetime.now()
			return {"start":startTime, "end":endTime}
		
		url = 'https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json'													
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		r=r['data']['stations']
		data_arry=[]
		for i in tqdm(list(range(len(r)))):
			lat=r[i]['lat']
			lon=r[i]['lon']																							#Trivial Selection
			data_arry.append({"lat":lat,"lon":lon})																	#Trivial Projection
		repo.dropCollection("washington_s")
		repo.createCollection("washington_s")
		repo['charr_hu38_npearce.washington_s'].insert_many(data_arry)												#Data set 2: Washington
		repo['charr_hu38_npearce.washington_s'].metadata({'complete':True})
		
		url = 'https://gbfs.citibikenyc.com/gbfs/en/station_information.json'													
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		r=r['data']['stations']
		data_arry=[]
		for i in tqdm(list(range(len(r)))):
			lat=r[i]['lat']
			lon=r[i]['lon']																							#Trivial Selection
			data_arry.append({"lat":lat,"lon":lon})																	#Trivial Projection
		repo.dropCollection("newyork_s")
		repo.createCollection("newyork_s")
		repo['charr_hu38_npearce.newyork_s'].insert_many(data_arry)													#Data set 3: New York
		repo['charr_hu38_npearce.newyork_s'].metadata({'complete':True})

		url = 'https://feeds.divvybikes.com/stations/stations.json'													
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		r=r['stationBeanList']
		data_arry=[]
		for i in tqdm(list(range(len(r)))):
			lat=r[i]['latitude']
			lon=r[i]['longitude']																					#Trivial Selection
			data_arry.append({"lat":lat,"lon":lon})																	#Trivial Projection
		repo.dropCollection("chicago_s")
		repo.createCollection("chicago_s")
		repo['charr_hu38_npearce.chicago_s'].insert_many(data_arry)													#Data set 4: Chicago
		repo['charr_hu38_npearce.chicago_s'].metadata({'complete':True})
		
		url = 'https://gbfs.fordgobike.com/gbfs/en/station_information.json'													
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		r=r['data']['stations']
		data_arry=[]
		for i in tqdm(list(range(len(r)))):
			lat=r[i]['lat']
			lon=r[i]['lon']																							#Trivial Selection
			data_arry.append({"lat":lat,"lon":lon})																	#Trivial Projection
		repo.dropCollection("sanfran_s")
		repo.createCollection("sanfran_s")
		repo['charr_hu38_npearce.sanfran_s'].insert_many(data_arry)													#Data set 5: San Francisco
		repo['charr_hu38_npearce.sanfran_s'].metadata({'complete':True})
		
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

		this_script = doc.agent('alg:charr_hu38_npearce#StationDataCollection', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_boston_s = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_newyork_s = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_washington_s = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_chicago_s = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_sanfran_s = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_boston_s, this_script)
		doc.wasAssociatedWith(get_newyork_s, this_script)
		doc.wasAssociatedWith(get_washington_s, this_script)
		doc.wasAssociatedWith(get_chicago_s, this_script)
		doc.wasAssociatedWith(get_sanfran_s, this_script)
		doc.usage(get_boston_s, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval'
				  }
				  )
		doc.usage(get_newyork_s, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval'
				  }
				  )
		doc.usage(get_washington_s, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval',
				  }
				  )
		doc.usage(get_chicago_s, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval',
				  }
				  )
		doc.usage(get_sanfran_s, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval',
				  }
				  )

		boston_s = doc.entity('dat:charr_hu38_npearce#boston_s', {prov.model.PROV_LABEL:'Boston Station Data', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(boston_s, this_script)
		doc.wasGeneratedBy(boston_s, get_boston_s, endTime)
		doc.wasDerivedFrom(boston_s, resource, get_boston_s, get_boston_s, get_boston_s)
				  
		newyork_s = doc.entity('dat:charr_hu38_npearce#newyork_s', {prov.model.PROV_LABEL:'New York Station Data', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(newyork_s, this_script)
		doc.wasGeneratedBy(newyork_s, get_newyork_s, endTime)
		doc.wasDerivedFrom(newyork_s, resource, get_newyork_s, get_newyork_s, get_newyork_s)

		washington_s = doc.entity('dat:charr_hu38_npearce#washington_s', {prov.model.PROV_LABEL:'Washington Station Data', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(washington_s, this_script)
		doc.wasGeneratedBy(washington_s, get_washington_s, endTime)
		doc.wasDerivedFrom(washington_s, resource, get_washington_s, get_washington_s, get_washington_s)
		
		chicago_s = doc.entity('dat:charr_hu38_npearce#chicago_s', {prov.model.PROV_LABEL:'Chicago Station Data', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(chicago_s, this_script)
		doc.wasGeneratedBy(chicago_s, get_chicago_s, endTime)
		doc.wasDerivedFrom(chicago_s, resource, get_chicago_s, get_chicago_s, get_chicago_s)
		
		sanfran_s = doc.entity('dat:charr_hu38_npearce#sanfran_s', {prov.model.PROV_LABEL:'San Francisco Station Data', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(sanfran_s, this_script)
		doc.wasGeneratedBy(sanfran_s, get_sanfran_s, endTime)
		doc.wasDerivedFrom(sanfran_s, resource, get_sanfran_s, get_sanfran_s, get_sanfran_s)
		
		repo.logout()
				  
		return doc


# This is DataCollection code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
#StationDataCollection.execute()
#doc = StationDataCollection.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof