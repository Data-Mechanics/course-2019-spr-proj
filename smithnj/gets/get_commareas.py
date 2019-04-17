import urllib.request
import json
import prov.model
import datetime
import uuid
import pandas as pd
from sodapy import Socrata
import dml

############################################
# get_commareas.py
# Script for collecting Chicago Community Areas Numbers
############################################

class get_commareas(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.commareas']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.commareas'
        # ---[ Grab Data ]-------------------------------------------
        client = Socrata("data.cityofchicago.org", "xbEYuk5XxkYsIaXl3hn79XIoR")
        results = client.get("74p9-q2aq", limit=500)
        df = pd.DataFrame.from_records(results).to_json(orient="records")
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('smithnj.commareas')
        repo.createCollection('smithnj.commareas')
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/commareas') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/commareas')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont','http://datamechanics.io/ontology#')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=smithnj/')

        this_script = doc.agent('alg:smithnj#get_commareas',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:commareas',
                              {'prov:label': 'data set of chicago community areas', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_commareas = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_commareas, this_script)
        doc.usage(get_commareas, resource, startTime, None)

        commmareas = doc.entity('dat:smithnj#commareas',
                          {prov.model.PROV_LABEL: 'Community Areas', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(commmareas, this_script)
        doc.wasGeneratedBy(commmareas, get_commareas, endTime)
        doc.wasDerivedFrom(commmareas, resource, get_commareas, get_commareas, get_commareas)

        repo.logout()

        return doc