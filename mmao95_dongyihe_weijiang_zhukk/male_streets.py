import requests
import json
import dml
import prov.model
import datetime
import uuid
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np

class male_streets(dml.Algorithm):
    contributor = 'mmao95_dongyihe_weijiang_zhukk'
    reads = [contributor + '.filtered_famous_people_streets']
    writes = [contributor + '.male_streets']

    @staticmethod
    def execute(trial = False):
        contributor = 'mmao95_dongyihe_weijiang_zhukk'
        reads = [contributor + '.filtered_famous_people_streets']
        writes = [contributor + '.male_streets']
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        streets = repo[reads[0]]
        male_streets = streets.find({'$and': [{'Gender': 'male'}, {'Gender2': 'M'}]})

        repo.dropCollection('male_streets')
        repo.createCollection('male_streets')
        repo[writes[0]].insert_many(list(male_streets))

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

        contributor = 'mmao95_dongyihe_weijiang_zhukk'
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:'+contributor+'#male_streets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        streets = doc.entity('dat:'+contributor+'#filtered_famous_people_streets', {prov.model.PROV_LABEL:'Streets without Famous People', prov.model.PROV_TYPE:'ont:DataSet'})
        filter_gender = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(filter_gender, this_script)
        doc.usage(filter_gender, streets, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Computation':'Filter'
            }
        )

        male_streets = doc.entity('dat:'+contributor+'#male_streets', {prov.model.PROV_LABEL:'Male Streets', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(male_streets, this_script)
        doc.wasGeneratedBy(male_streets, filter_gender, endTime)
        doc.wasDerivedFrom(male_streets, streets, filter_gender, filter_gender, filter_gender)

        repo.logout()
                  
        return doc

## eof
