import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from sodapy import Socrata

############################################
# get_stationsttats.py
# Script for collecting CTA Station Ridership data
############################################

class get_stationstats(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.ctastats']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.ctastats'
        # ---[ Grab Data ]-------------------------------------------
        client = Socrata("data.cityofchicago.org", "xbEYuk5XxkYsIaXl3hn79XIoR")
        if (trial):
            results = client.get("t2rn-p8d7", select="station_id,stationame,month_beginning,monthtotal", where="month_beginning > '2012-12-01T00:00:00.000' AND month_beginning < '2019-01-01T00:00:00.000'", limit=100)
        if (trial == False):
            results = client.get("t2rn-p8d7", select="station_id,stationame,month_beginning,monthtotal", where="month_beginning > '2012-12-01T00:00:00.000' AND month_beginning < '2019-01-01T00:00:00.000'", limit=30000)
        df = pd.DataFrame.from_records(results).to_json(orient="records")
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('ctastats')
        repo.createCollection('ctastats')
        print('done')
        repo[repo_name].insert_many(loaded)
        repo[repo_name].metadata({'complete': True})
        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/ctastats') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/ctastats')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont','http://datamechanics.io/ontology#')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=smithnj/')

        this_script = doc.agent('alg:smithnj#get_stationstats',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:stationstats',
                              {'prov:label': 'data set of l station statistics', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_stationstats = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_stationstats, this_script)
        doc.usage(get_stationstats, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '$select=station_id,stationame,month_beginning,monthtotal'
                   }
                  )

        ctastats = doc.entity('dat:smithnj#ctastats',
                          {prov.model.PROV_LABEL: 'L Stations Ridership Statistics', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(ctastats, this_script)
        doc.wasGeneratedBy(ctastats, get_stationstats, endTime)
        doc.wasDerivedFrom(ctastats, resource, get_stationstats, get_stationstats, get_stationstats)

        repo.logout()

        return doc