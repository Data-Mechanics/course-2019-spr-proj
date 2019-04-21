import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import urllib.request
import pandas as pd

class population(dml.Algorithm):
  contributor = 'signior_jmu22.zhangyb'
  reads = []
  writes = ['signior_jmu22_zhangyb.population']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    # Set up database connection
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22_zhangyb', 'signior_jmu22_zhangyb')

    url = 'http://datamechanics.io/data/signior_jmu22/yearly_population.csv' # grab dataset from datamechanics.io
    df = pd.read_csv(url, sep='\t')
    filter_values = ['Country Name']
    for i in range(1960, 2015):
      filter_values.append(str(i))
    new_df = df.filter(filter_values)
    
    population_dict = new_df.to_dict(orient="records")
    repo.dropCollection("population")
    repo.createCollection("population")
    repo['signior_jmu22_zhangyb.population'].insert_many(population_dict)
    repo['signior_jmu22_zhangyb.population'].metadata({'complete': True})

    print(repo['signior_jmu22_zhangyb.population'].metadata())

    repo.logout()

    endTime = datetime.datetime.now()
    return {"start": startTime, "end": endTime}
    
  
  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22_zhangyb', 'signior_jmu22_zhangyb')
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/signior_jmu22') # The scripts are in <folder>#<filename> format
    doc.add_namespace('dat', 'http://datamechanics.io/data/signior_jmu22' ) # The datasets are in <user>#<collection> format
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retreival', 'Query', or 'Computation'
    doc.add_namespace('log', 'http://datamechanics.io/log/')

    this_script = doc.agent('alg:signior_jmu22#population', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
    resource = doc.entity('dat:population', {'prov:label': 'Yearly Population by Country 1960 - 2017', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'csv'})
    get_population = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_population, this_script)
    doc.usage(get_population, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retreival'})
    
    population = doc.entity('dat:signior_jmu22#population', {prov.model.PROV_LABEL: 'Population by Country (yearly)', prov.model.PROV_TYPE: 'ont:DataSet'})
    doc.wasAttributedTo(population, this_script)
    doc.wasGeneratedBy(population, get_population, endTime)
    doc.wasDerivedFrom(population, resource, get_population, get_population, get_population)

    repo.logout()


    return doc



# comment this when submitting, just for testing purposes
# population.execute()
# doc = population.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
  