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

class Kmeans(dml.Algorithm):
	contributor = 'charr_hu38_npearce'
	reads = ['charr_hu38_npearce.boston_s', 'charr_hu38_npearce.washington_s', 'charr_hu38_npearce.newyork_s', 'charr_hu38_npearce.chicago_s', 'charr_hu38_npearce.sanfran_s']
	writes = ['charr_hu38_npearce.Kmeans']

	def dist(p, q):
		(x1,y1) = p
		(x2,y2) = q
		return (x1-x2)**2 + (y1-y2)**2

	def plus(args):
		p = [0,0]
		for (x,y) in args:
			p[0] += x
			p[1] += y
		return tuple(p)

	def scale(p, c):
		(x,y) = p
		return (x/c, y/c)
	
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
		
		boston_s = list(repo.charr_hu38_npearce.census.find())
		washington_s = list(repo.charr_hu38_npearce.census.find())
		newyork_s = list(repo.charr_hu38_npearce.census.find())
		chicago_s = list(repo.charr_hu38_npearce.census.find())
		sanfran_s = list(repo.charr_hu38_npearce.census.find())
		
		#K-means Code
		M = [(13,1), (2,12)]
		P = [(1,2),(4,5),(1,3),(10,12),(13,14),(13,9),(11,11)]

		OLD = []
		while OLD != M:
			OLD = M

			MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
			PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
			PD = aggregate(PDs, min)
			MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
			MT = aggregate(MP, plus)
	
			M1 = [(m, 1) for (m, _) in MP]
			MC = aggregate(M1, sum)

			M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]

		data_arry=[]
		for city1 in aggbikedata:																										#Product
			for city2 in census:
				if city1['city'] == city2['location']:																					#Selection
					data_arry.append({"city":city1['city'],"tot_bike_time":city1['tot_bike_time'],"population":city2['population']})	#Projection
					break
		
		
		repo['charr_hu38_npearce.Kmeans'].insert_many(data_arry)							#Join on two data sets (Non Trivial Transformation #3)
		repo['charr_hu38_npearce.Kmeans'].metadata({'complete':True})
		
		#We treat this as a single nontrivial transformation, as it is a join that involves a product, selection, and projection

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

		Kmeans = doc.entity('dat:charr_hu38_npearce#Kmeans', {prov.model.PROV_LABEL:'Total time spent on bike and population data for 4 cities', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(Kmeans, this_script)
		doc.wasGeneratedBy(Kmeans, get_Kmeans, endTime)
		doc.wasDerivedFrom(Kmeans, resource, get_Kmeans, get_Kmeans, get_Kmeans)
		
		repo.logout()
				  
		return doc
		
'''
# This is Kmeans code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
Kmeans.execute()
doc = Kmeans.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof