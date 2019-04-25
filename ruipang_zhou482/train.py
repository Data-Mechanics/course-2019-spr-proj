import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
import os 
import random
from sklearn.linear_model import LinearRegression
def aggregate(R, f):
	keys = {r[0] for r in R}
	return [(key, f([v for (k,v) in R if k == key])) for key in keys]
class train(dml.Algorithm):

	contributor = 'ruipang_zhou482'
	reads = ["ruipang_zhou482.hospital", "ruipang_zhou482.police", "ruipang_zhou482.privateSchool", "ruipang_zhou482.publicSchool", "ruipang_zhou482.propertyAssessment"]
	writes = []



	@staticmethod
	def execute(trial = False):

		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('ruipang_zhou482', 'ruipang_zhou482')

		privateSchool = {}
		for i in repo['ruipang_zhou482.privateSchool'].find():
			print(i)
			privateSchool[i["zipcode"]] = i["num_school"]
        
		publicSchool = {}
		for i in repo['ruipang_zhou482.publicSchool'].find():
			publicSchool[i["zipcode"]] = i["num_school"]
		propertyAssessment = {}
		for i in repo['ruipang_zhou482.propertyAssessment'].find():
			propertyAssessment[i["zipcode"]] = i["avg_value"]
		police = {}
		for i in repo['ruipang_zhou482.police'].find():
			police[i["zipcode"]] = i["count"]
		hospital = {}
		for i in repo['ruipang_zhou482.hospital'].find():
			hospital[i["zipcode"]] = i["count"]

		data = {}
		for zipcode in propertyAssessment:
			row = {}
			if zipcode in publicSchool:
				row["publicSchool"] = publicSchool[zipcode]
			else:
				row["publicSchool"] = 0
			if zipcode in privateSchool:
				row["privateSchool"] = privateSchool[zipcode]
			else:
				row["privateSchool"] = 0
			if zipcode in police:
				row["police"] = police[zipcode]
			else:
				row["police"] = 0
			if zipcode in hospital:
				row["hospital"] = hospital[zipcode]
			else:
				row["hospital"] = 0
			row["propertyAssessment"] = propertyAssessment[zipcode]
			data[zipcode] = row
		if trial == True:
			print("Train.py running in trial mode")
			toDelete = []
			for key in data:
				r = random.random()
				if r < 0.5:
					toDelete.append(key)
			for key in toDelete:
				data.pop(key)
		X = []
		y = []
		for zipcode in data:
			X.append([data[zipcode]["publicSchool"], data[zipcode]["privateSchool"], data[zipcode]["police"], data[zipcode]["hospital"]])
			y.append(data[zipcode]["propertyAssessment"])
		reg = LinearRegression().fit(X, y)
		f = open("ruipang_zhou482/out.txt", "a")
		f.write("Linear Regression Coefficient:" + str(reg.coef_[0]) + " " + str(reg.coef_[1]) + " " + str(reg.coef_[2]) + " " + str(reg.coef_[3]) + "\n")
		f.write("Linear Regression Intercept:" + str(reg.intercept_) + "\n")
			

		
		
		repo.logout()

		endTime = datetime.datetime.now()
		return {"start": startTime, "end": endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('ruipang_zhou482', 'ruipang_zhou482')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:train', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource1 = doc.entity('dat:privateSchool', {'prov:label':'privateSchool', prov.model.PROV_TYPE:'ont:DataResource'})
		resource2 = doc.entity('dat:publicSchool', {'prov:label':'publicSchool', prov.model.PROV_TYPE:'ont:DataResource'})
		resource3 = doc.entity('dat:police', {'prov:label':'police', prov.model.PROV_TYPE:'ont:DataResource'})
		resource4 = doc.entity('dat:hospital', {'prov:label':'hospital', prov.model.PROV_TYPE:'ont:DataResource'})
		resource5 = doc.entity('dat:propertyAssessment', {'prov:label':'propertyAssessment', prov.model.PROV_TYPE:'ont:DataResource'})
		train_X = doc.entity('dat:train_X', {prov.model.PROV_LABEL:'Training set', prov.model.PROV_TYPE:'ont:DataSet'})
		train_y = doc.entity('dat:train_y', {prov.model.PROV_LABEL:'Training set', prov.model.PROV_TYPE:'ont:DataSet'})

		doc.wasAttributedTo(train_X, this_script)
		doc.wasAttributedTo(train_y, this_script)
		doc.wasDerivedFrom(train_X, resource1, resource2, resource3, resource4)
		doc.wasDerivedFrom(train_X, resource5)

		repo.logout()

		return doc
