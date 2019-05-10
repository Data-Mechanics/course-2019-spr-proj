import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import urllib.request
import pandas as pd
import math

class car_data(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = []
  writes = ['signior_jmu22.car_data']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    # Set up database connection
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')

    url = 'http://datamechanics.io/data/signior_jmu22/car_data.csv' # grab dataset from datamechanics.io
    df = pd.read_csv(url)
    filter_values = ['Country Name']
    for i in range(1960, 2015):
      filter_values.append(str(i))
    new_df = df.filter(filter_values)
    car_data_dict = new_df.to_dict(orient='records')
    
    car_data_new = []
    # iterate through dict and delete years with nan
    for row in car_data_dict:
      new_row = {}
      new_row['Country Name'] = row['Country Name']
      for key in row:
        try:
          year_key = str(key)
          value = row[year_key]
          if (isinstance(value, float) and not math.isnan(value)):
            new_row[year_key] = value
        except:
          continue
      car_data_new.append(new_row)
    
    # print(car_data_new)
    # print(car_data_new)
    repo.dropCollection("car_data")
    repo.createCollection("car_data")

    repo['signior_jmu22.car_data'].insert_many(car_data_new)
    repo['signior_jmu22.car_data'].metadata({'complete': True})

    print(repo['signior_jmu22.car_data'].metadata())

    repo.logout()

    endTime = datetime.datetime.now()

    return {"start": startTime, "end": endTime}
    
  
  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')
    
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/signior_jmu22') # The scripts are in <folder>#<filename> format
    doc.add_namespace('dat', 'http://datamechanics.io/data/signior_jmu22' ) # The datasets are in <user>#<collection> format
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retreival', 'Query', or 'Computation'
    doc.add_namespace('log', 'http://datamechanics.io/log/')

    this_script = doc.agent('alg:signior_jmu22#car_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
    resource = doc.entity('dat:car_data', {'prov:label': 'Number Cars per 1000 people by Country 1961 - 2013', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'csv'})
    get_car_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_car_data, this_script)
    doc.usage(get_car_data, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retreival'})

    car_data = doc.entity('dat:signior_jmu22#car_data', {prov.model.PROV_LABEL: 'Car data by Country', prov.model.PROV_TYPE: 'ont:DataSet'})
    doc.wasAttributedTo(car_data, this_script)
    doc.wasGeneratedBy(car_data, get_car_data, endTime)
    doc.wasDerivedFrom(car_data, resource, get_car_data, get_car_data, get_car_data)

    return doc



# # comment this when submitting, just for testing purposes
# car_data.execute()
# doc = car_data.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))