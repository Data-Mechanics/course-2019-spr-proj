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
class create_taxiagg(dml.Algorithm):

    contributor = 'smithnj'
    reads = ['smithnj.taxistats']
    writes = ['smithnj.taxiagg']

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
        repo_name = 'smithnj.taxiagg'
        # ---[ Initialize Cursor & Collection of TaxiStats]----------
        taxistats = repo.smithnj.taxistats
        taxistats_cursor = repo.smithnj.taxistats.find()
        # ---[ Transformations ]-------------------------------------
        pickup_counts_temp = []
        dropoff_counts_temp = []
        for i in taxistats_cursor:
            dropoff_counts_temp.append((i['dropoff_community_area'], 1))
            pickup_counts_temp.append((i['pickup_community_area'], 1))
        dropoff_counts = aggregate(dropoff_counts_temp, sum)
        pickup_counts = aggregate(pickup_counts_temp, sum)
        totals = [(d, (b + (a*0.5))) for ((d, a), (p, b)) in product(dropoff_counts, pickup_counts) if d == p] #Dropoffs count for 1/2 since fares would only be based on entry location.
        # ---[ Send to Dict ]----------------------------------------
        labels = lambda t: {'Community Area': t[0], 'Demand Metric': t[1]}
        result = project(totals, labels)
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/taxiagg')
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/taxiagg')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=smithnj/')

        this_script = doc.agent('alg:smithnj#taxiagg',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:taxistats',
                              {'prov:label': 'data set of chicago taxi trip ID, pickup, and dropoff',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        transformation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transformation, this_script)
        doc.usage(transformation, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        create_taxiagg = doc.entity('dat:smithnj#taxiagg',
                                              {prov.model.PROV_LABEL: 'get_taxiagg',
                                               prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(create_taxiagg, this_script)
        doc.wasGeneratedBy(create_taxiagg, transformation, endTime)
        doc.wasDerivedFrom(create_taxiagg, resource, transformation, transformation, transformation)

        repo.logout()
        return doc