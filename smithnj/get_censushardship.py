import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from sodapy import Socrata

############################################
# get_censushardship.py
# Script for collecting Chicago Census Socioeconomic Indicators
############################################

class get_censushardship(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.census']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.census'
        # ---[ Grab Data ]-------------------------------------------
        client = Socrata("data.cityofchicago.org", "xbEYuk5XxkYsIaXl3hn79XIoR")
        results = client.get("kn9c-c2s2", select="ca,hardship_index", limit=1000)
        df = pd.DataFrame.from_records(results).to_json(orient="records")
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection(repo_name)
        repo.createCollection(repo_name)
        print('done')
        repo[repo_name].insert_many(loaded)
        repo[repo_name].metadata({'complete': True})
        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/census') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/census')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont','http://datamechanics.io/ontology#')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=smithnj/')

        this_script = doc.agent('alg:smithnj#get_censushardship',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:census',
                              {'prov:label': 'data set of census socioeconimc hardship per community area', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_censushardship = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_censushardship, this_script)
        doc.usage(get_censushardship, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?select=ca,hardship_index'
                   }
                  )

        census = doc.entity('dat:smithnj#census',
                          {prov.model.PROV_LABEL: 'Census Hardship', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(census, this_script)
        doc.wasGeneratedBy(census, get_censushardship, endTime)
        doc.wasDerivedFrom(census, resource, get_censushardship, get_censushardship, get_censushardship)

        repo.logout()

        return doc