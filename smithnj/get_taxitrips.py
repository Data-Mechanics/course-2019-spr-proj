import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from sodapy import Socrata

############################################
# get_taxitrips.py
# Script for collecting Chicago Taxi Trip data
############################################

class get_taxitrips(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.taxistats']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.taxistats'
        # ---[ Grab Data ]-------------------------------------------
        client = Socrata("data.cityofchicago.org", "xbEYuk5XxkYsIaXl3hn79XIoR")
        if trial:
            results = client.get("wrvz-psew", select="trip_id,pickup_community_area,dropoff_community_area", where="trip_start_timestamp > '2013-01-01T00:00:00.000' AND trip_start_timestamp < '2019-01-01T00:00:00.000'", limit=10)
        if trial == False:
            results = client.get("wrvz-psew", select="trip_id,pickup_community_area,dropoff_community_area", where="trip_start_timestamp > '2013-01-01T00:00:00.000' AND trip_start_timestamp < '2019-01-01T00:00:00.000'", limit=25000000)
        df = pd.DataFrame.from_records(results).to_json(orient="records")
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('taxistats')
        repo.createCollection('taxistats')
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/taxistats') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/taxistats')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont','http://datamechanics.io/ontology#')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=smithnj/')

        this_script = doc.agent('alg:smithnj#get_taxitrips',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:taxistats',
                              {'prov:label': 'data set of chicago taxi trip ID, pickup, and dropoff', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_taxitrips = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_taxitrips, this_script)
        doc.usage(get_taxitrips, resource, startTime, None)

        taxistats = doc.entity('dat:smithnj#taxistats',
                          {prov.model.PROV_LABEL: 'Taxi Stats', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(taxistats, this_script)
        doc.wasGeneratedBy(taxistats, get_taxitrips, endTime)
        doc.wasDerivedFrom(taxistats, resource, get_taxitrips, get_taxitrips, get_taxitrips)

        repo.logout()

        return doc