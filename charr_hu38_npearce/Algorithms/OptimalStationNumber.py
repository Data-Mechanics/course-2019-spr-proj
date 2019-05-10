import dml
import prov.model
import datetime
import uuid
import numpy as np
from sklearn.linear_model import LinearRegression

class OptimalStationNumber(dml.Algorithm):
	contributor = 'charr_hu38_npearce'
	reads = ['charr_hu38_npearce.unionpopbike']
	writes = ['charr_hu38_npearce.optstationnum']

	@staticmethod
	def execute(trial = False, max_num=10):
		'''Union dataset containing bike data into the dataset containing city census information'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('charr_hu38_npearce', 'charr_hu38_npearce')

		repo.dropCollection("optstationnum")
		repo.createCollection("optstationnum")
		
		data = repo.charr_hu38_npearce.unionpopbike.find()
		
		cities=[]
		X = []
		y = []
		for city in data:
			cities.append(city['city'])
			X.append(int(city['stations'])/int(city['population'])) #Amount of bike stations per person
			y.append(city['tot_bike_time']/int(city['population'])) #Time spent on bike per person
		
		z = np.polyfit(X,y,1)		#Linear regression
		c = z[0]
		
		#It is clear from our data that adding stations only increases productivity, so we add the max we can build
		#k-means k=max_num
		k=max_num
		data_arry=[]
		for i in range(len(X)):
			data_arry.append({"opt_num":k,"coef":c,"x":X[i],"y":y[i]})
		repo['charr_hu38_npearce.optstationnum'].insert_many(data_arry)						#Data set 0: Optimal Station Number
		repo['charr_hu38_npearce.optstationnum'].metadata({'complete':True})

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

		this_script = doc.agent('alg:charr_hu38_npearce#OptimalStationNumber', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Linear Regression for optimal station number', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		
		get_OptimalStationNumber = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_OptimalStationNumber, this_script)
		doc.usage(get_OptimalStationNumber, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'
				  }
				  )

		OptimalStationNumber = doc.entity('dat:charr_hu38_npearce#OptimalStationNumber', {prov.model.PROV_LABEL:'Linear Regression for optimal station number', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(OptimalStationNumber, this_script)
		doc.wasGeneratedBy(OptimalStationNumber, get_OptimalStationNumber, endTime)
		doc.wasDerivedFrom(OptimalStationNumber, resource, get_OptimalStationNumber, get_OptimalStationNumber, get_OptimalStationNumber)
		
		repo.logout()
				  
		return doc
		

# This is OptimalStationNumber code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
#OptimalStationNumber.execute()
#doc = OptimalStationNumber.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof