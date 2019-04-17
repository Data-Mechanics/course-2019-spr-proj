import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from pprint import pprint

"""

"""


class pairwiseCorrelation(dml.Algorithm):
    contributor = 'nhuang54_tkixi_wud'
    reads = ['nhuang54_tkixi_wud.streetlight_collisions']
    writes = ['nhuang54_tkixi_wud.correlation']


    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('nhuang54_tkixi_wud', 'nhuang54_tkixi_wud')
        
        tc = repo.nhuang54_tkixi_wud.streetlight_collisions.find() 
        data = []
        for item in tc:
            data.append({'streetlight':item.get('streetlight'),
                            'collisions':item.get('collisions')})
        if trial:
            data = data[:20]
            
        data = pd.DataFrame(data) # constructs DataFrame with data

        corr = pd.DataFrame(data.corr()) # Compute pairwise correlation of columns
        pprint(corr)

        repo.dropCollection("correlation")
        repo.createCollection("correlation")

        r = {'field1': 'streetlight', 'field2': 'collisions', 'value': corr['streetlight']['collisions']}
        pprint(r)
        repo['nhuang54_tkixi_wud.correlation'].insert(r)
        repo['nhuang54_tkixi_wud.correlation'].metadata({'complete':True})
        print(repo['nhuang54_tkixi_wud.correlation'].metadata())


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
        repo.authenticate('nhuang54_tkixi_wud', 'nhuang54_tkixi_wud')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:nhuang54_tkixi_wud#pairwiseCorrelation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource2 = doc.entity('dat:nhuang54_tkixi_wud#streetlights', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        getCorrelation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getCorrelation, this_script)

        doc.usage(getCorrelation, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )


        bikestreetlight_correlation = doc.entity('dat:nhuang54_tkixi_wud#bikestreetlight_correlation', {prov.model.PROV_LABEL:'Correlation of number of bike incidents and street lights present', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikestreetlight_correlation, this_script)
        doc.wasGeneratedBy(bikestreetlight_correlation, getCorrelation, endTime)
        doc.wasDerivedFrom(bikestreetlight_correlation, resource1, resource2, getCorrelation, getCorrelation)

        repo.logout()
                  
        return doc
if __name__ == '__main__':
    pairwiseCorrelation.execute(trial=True)

