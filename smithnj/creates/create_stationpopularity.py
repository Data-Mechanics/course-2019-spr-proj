import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
############################################
# create_stationpopularity.py
# Script for creating station sum of entries and difference from average entries.
############################################
class create_stationpopularity(dml.Algorithm):

    contributor = 'smithnj'
    reads = ['smithnj.ctastats']
    writes = ['smithnj.ctapopularity']

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
        repo_name = 'smithnj.ctapopularity'
        # ---[ Initialize Cursor & Collection of TaxiStats]----------
        ctastats = repo.smithnj.ctastats
        ctastats_cursor = repo.smithnj.ctastats.find()
        # ---[ Transformations ]-------------------------------------
        newstats = []
        for i in ctastats_cursor:
            newstats.append((i["station_id"], i["month_beginning"][0:4], int(i["monthtotal"])))
        newstats_adj = [(station, total) for (station, year, total) in newstats]
        newstats_sums = aggregate(newstats_adj, sum)
        # ---[ Send to Dict ]----------------------------------------
        labels = lambda t: {'Station ID': t[0], 'Total Ridership': t[1]}
        result = project(newstats_sums, labels)
        # ---[ MongoDB Insertion ]-----------------------------------
        df = pd.DataFrame.from_dict(result)
        average = df["Total Ridership"].mean()
        df["VarMean"] = df["Total Ridership"] - average
        df = df.to_json(orient="records")
        loaded = json.loads(df)
        repo.dropCollection('ctapopularity')
        repo.createCollection('ctapopularity')
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/ctapopularity')
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/ctapopularity')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=smithnj/')

        this_script = doc.agent('alg:smithnj#ctapopularity',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:ctastats',
                              {'prov:label': 'data set of l station statistics',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        transformation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transformation, this_script)
        doc.usage(transformation, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        get_stationpopularity = doc.entity('dat:smithnj#ctapopularity',
                                              {prov.model.PROV_LABEL: 'get_taxiagg',
                                               prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(get_stationpopularity, this_script)
        doc.wasGeneratedBy(get_stationpopularity, transformation, endTime)
        doc.wasDerivedFrom(get_stationpopularity, resource, transformation, transformation, transformation)

        repo.logout()
        return doc