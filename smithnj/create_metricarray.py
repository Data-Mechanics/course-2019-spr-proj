import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
############################################
# create_taxiagg.py
# Script for creating taxi community area pickup and dropoff counts.
############################################
class create_metricarray(dml.Algorithm):

    contributor = 'smithnj'
    reads = ['smithnj.ctapopularity', 'smithnj.stationhardship', 'smithnj.taxiagg']
    writes = ['smithnj.metrics']

    @staticmethod
    def execute(trial = False):
        # ---[ Assistant Functions ]---------------------------------
        def project(R, p):
            return [p(t) for t in R]

        def product(R, S):
            return [(t, u) for t in R for u in S]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k, v) in R if k == key])) for key in keys]

        startTime = datetime.datetime.now()
        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.metrics'
        # ---[ Initialize Cursors ]----------------------------------
        popularity_cursor = repo.smithnj.ctapopularity.find()  # Station ID, Ridership Total, Difference from Mean
        hardship_cursor = repo.smithnj.stationhardship.find()  # CA, Hardship, Station ID
        taxi_cursor = repo.smithnj.taxiagg.find()  # CA, Dropoffs, Pickups
        # ---[ Transformations ]-------------------------------------
        taxi_list = []
        hardship_list = []
        for i in taxi_cursor:
            taxi_list.append((i["Community Area"], (i["Demand Metric"])))
        for i in hardship_cursor:
            hardship_list.append((i["CA"], i["Station ID"], i["Hardship"]))
        station_hardship_taxi = [(station, int(hardship), taxi) for ((communityarea, taxi), (ca, station, hardship)) in
                                 product(taxi_list, hardship_list) if int(communityarea) == int(ca)]
        nulls_station_hardship_taxi = [(station, None, None) for (ca, station, hardship) in hardship_list if ca == -1]
        merged_station_hardship_taxi = station_hardship_taxi + nulls_station_hardship_taxi
        popularity_list = []
        for i in popularity_cursor:
            popularity_list.append((i["Station ID"], i["VarMean"]))
        metrics = [(station, popularity, hardship, demand) for ((station, hardship, demand), (stationID, popularity)) in
                   product(merged_station_hardship_taxi, popularity_list) if station == stationID]
        # ---[ Send to Dict ]----------------------------------------
        labels = lambda t: {'Station ID': t[0], 'Popularity': t[1], 'Hardship Index': t[2], 'Taxi Demand': t[3]}
        result = project(metrics, labels)
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/metrics')
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/metrics')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=smithnj/')

        this_script = doc.agent('alg:smithnj#metrics',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource1 = doc.entity('bdp:ctapopularity',
                              {'prov:label': 'data set of cta station popularity',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource2 = doc.entity('bdp:stationhardship',
                              {'prov:label': 'data set of cta station and the community areas hardship',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource3 = doc.entity('bdp:taxiagg',
                              {'prov:label': 'data set of taxi demand in a community area',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        transformation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transformation, this_script)
        doc.usage(transformation, resource1, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(transformation, resource2, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(transformation, resource3, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        create_metricarray = doc.entity('dat:smithnj#metrics',
                                              {prov.model.PROV_LABEL: 'create_metricarray',
                                               prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(create_metricarray, this_script)
        doc.wasGeneratedBy(create_metricarray, transformation, endTime)
        doc.wasDerivedFrom(create_metricarray, resource1, resource2, resource3, transformation, transformation)

        repo.logout()
        return doc