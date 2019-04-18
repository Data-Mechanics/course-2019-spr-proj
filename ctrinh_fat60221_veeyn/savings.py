import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class savings(dml.Algorithm):
    contributor = 'ctrinh_fat60221_veeyn'
    reads = ['ctrinh_fat60221_veeyn.uber', 'ctrinh_fat60221_veeyn.getMean', 'ctrinh_fat60221_veeyn.getNewMean']
    writes = ['ctrinh_fat60221_veeyn.savings']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')

        uber = list(repo['ctrinh_fat60221_veeyn.uber'].find({}))
        mean = list(repo['ctrinh_fat60221_veeyn.getMean'].find({}))
        newMean = list(repo['ctrinh_fat60221_veeyn.getNewMean'].find({}))

        # print(uber[1])
        mean_value = mean[0]['MEAN']
        newMean_value = newMean[0]['newMEAN']
        fracDec = newMean_value / mean_value
        # print(mean_value)
        # print(newMean_value)
        # print(fracDec)

        r = []
        if trial:
            for entry in uber[:3]:
                rdict = {}
                rdict['hour_of_day'] = entry['hod']
                rdict['aggregate_mean_travel_hours'] = entry['aggregate_mean_travel_times'] / 60 / 60
                rdict['theoretical_mean_travel_hours'] = rdict['aggregate_mean_travel_hours'] - (rdict['aggregate_mean_travel_hours'] * fracDec)
                rdict['theoretical_mean_travel_hours_saved'] = rdict['aggregate_mean_travel_hours'] * fracDec
                rdict['theoretical_percentage_change'] = fracDec
                r.append(rdict)
        else:
            for entry in uber:
                rdict = {}
                rdict['hour_of_day'] = entry['hod']
                rdict['aggregate_mean_travel_hours'] = entry['aggregate_mean_travel_times'] / 60 / 60
                rdict['theoretical_mean_travel_hours'] = rdict['aggregate_mean_travel_hours'] - (rdict['aggregate_mean_travel_hours'] * fracDec)
                rdict['theoretical_mean_travel_hours_saved'] = rdict['aggregate_mean_travel_hours'] * fracDec
                rdict['theoretical_percentage_change'] = fracDec
                r.append(rdict)

        print(r)


        # df = df1.filter(['hod', 'mean_travel_time'])
        # r = df.to_dict(orient='records')

        # print(r)
        # rdf = pd.DataFrame(r)

        # grouped = rdf.groupby(['hod']).agg(sum)
        
        # grouped.columns = ['hod', 'aggregate_mean_travel_times']

        # r = grouped.reset_index().to_dict('records')

        # for i in range(len(r)):
        #     r[i]["aggregate_mean_travel_times"] = r[i].pop('mean_travel_time')
        # print(r)

        # print(df2.to_dict(orient='records')[0])
        # print(df2[1:])
        # print(r[-2])

        repo.dropCollection("savings")
        repo.createCollection("savings")
        repo['ctrinh_fat60221_veeyn.savings'].insert_many(r)
        repo['ctrinh_fat60221_veeyn.savings'].metadata({'complete':True})
        print(repo['ctrinh_fat60221_veeyn.savings'].metadata())

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
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dmc', 'http://datamechanics.io/data/')

        this_script = doc.agent('alg:ctrinh_fat60221_veeyn#savings', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('alg:ctrinh_fat60221_veeyn', {'prov:label':'Uber / K-Means', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'py'})
        get_savings = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_savings, this_script)
        doc.usage(get_savings, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        savings = doc.entity('dat:ctrinh_fat60221_veeyn#savings', {prov.model.PROV_LABEL:'Uber Mean Travel Time Theoretical Reduction', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(savings, this_script)
        doc.wasGeneratedBy(savings, get_savings, endTime)
        doc.wasDerivedFrom(savings, resource, get_savings, get_savings, get_savings)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# savings.execute()
# doc = savings.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
