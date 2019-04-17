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
        print('computing pairwise correlation')
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
            print('trial', data)
            
        data = pd.DataFrame(data) # constructs DataFrame with data
        # print('printing')
        # pprint(data)
        # print('done')

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

        
        # Agent, entity, activity
        this_script = doc.agent('alg:nhuang54_tkixi_wud#pairwiseCorrelation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # Resource = crimesData
        resource1 = doc.entity('dat:ferrys#streetlights', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        # Resource = crimesData
        resource2 = doc.entity('dat:nhuang54_tkixi_wud#sortedNeighborhoods', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        #Activity
        find_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(find_correlation, this_script)

        doc.usage(find_correlation, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )
        doc.usage(find_correlation, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )


        corr = doc.entity('dat:nhuang54_tkixi_wud#coorelation', {prov.model.PROV_LABEL:'Correlation between streetlights and crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(corr, this_script)
        doc.wasGeneratedBy(corr, find_correlation, endTime)
        doc.wasDerivedFrom(corr, resource1, resource2, find_correlation, find_correlation)

        repo.logout()
                  
        return doc
        
# if __name__ == '__main__':
#     pairwiseCorrelation.execute(trial=True)
pairwiseCorrelation.execute()

