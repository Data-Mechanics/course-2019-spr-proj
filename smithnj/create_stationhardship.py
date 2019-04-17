import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
############################################
# create_stationhardship.py
# Script for creating pairing of hardship to station (via community area number).
############################################
class create_stationhardship(dml.Algorithm):

    contributor = 'smithnj'
    reads = ['smithnj.census', 'smithnj.stations']
    writes = ['smithnj.stationhardship']

    @staticmethod
    def execute(trial = False):
        # ---[ Assistant Functions ]---------------------------------
        def project(R, p):
            return [p(t) for t in R]

        def product(R, S):
            return [(t, u) for t in R for u in S]

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.stationhardship'
        # ---[ Initialize Cursor & Collection of TaxiStats]----------
        stations = repo.smithnj.stations
        stations_cursor = repo.smithnj.stations.find()
        census = repo.smithnj.census
        census_cursor = repo.smithnj.census.find()
        # ---[ Transformations ]-------------------------------------
        new_stations = []
        new_census = []
        for i in stations_cursor:
            new_stations.append([i["CommareaN"], i["Station Id"]])
        for x in new_stations:
            x[1] = x[1] + 40000
            if x[0] is None:
                x[0] = "-1"
            x[0] = int(x[0])
            x[1] = str(x[1])
        for i in census_cursor:
            new_census.append([i["ca"], i["hardship_index"]])
        for x in new_census:
            if x[0] is None:
                x[0] = "-1"
            x[0] = int(x[0])
            x[1] = str(x[1])
        merged = [(station, hardship, ca) for ((ca, hardship), (ca_s, station)) in product(new_census, new_stations) if
                  ca == ca_s]
        # ---[ Send to Dict ]----------------------------------------
        labels = lambda t: {'Station ID': t[0], 'Hardship': t[1], 'CA': t[2]}
        result = project(merged, labels)
        print("done")
        # ---[ MongoDB Insertion ]-----------------------------------
        df = pd.DataFrame.from_dict(result).to_json(orient="records")
        loaded = json.loads(df)
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
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/stationhardship')
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/stationhardship')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=smithnj/')

        this_script = doc.agent('alg:smithnj#stationhardship',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource1 = doc.entity('bdp:census',
                              {'prov:label': 'data set of census socioeconimc hardship per community area',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource2 = doc.entity('bdp:stations',
                               {'prov:label': 'data set of l stations',
                                prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        transformation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transformation, this_script)
        doc.usage(transformation, resource1, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(transformation, resource2, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        create_stationhardship = doc.entity('dat:smithnj#stationhardship',
                                              {prov.model.PROV_LABEL: 'get_stationhardship',
                                               prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(create_stationhardship, this_script)
        doc.wasGeneratedBy(create_stationhardship, transformation, endTime)
        doc.wasDerivedFrom(create_stationhardship, resource1, resource2, transformation, transformation, transformation)

        repo.logout()
        return doc