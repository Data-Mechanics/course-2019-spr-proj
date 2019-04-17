import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class uber(dml.Algorithm):
    contributor = 'ctrinh_fat60221_veeyn'
    reads = []
    writes = ['ctrinh_fat60221_veeyn.uber']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')

        url1 = 'http://datamechanics.io/data/ctrinh_fat60221_veeyn/uber-peak-hourly-2018-1.csv'
        df1 = pd.read_csv(url1)

        # print(df1)

        url2 = 'http://datamechanics.io/data/ctrinh_fat60221_veeyn/uber-peak-hourly-2018-2.csv'
        df2 = pd.read_csv(url2)

        url3 = 'http://datamechanics.io/data/ctrinh_fat60221_veeyn/uber-peak-hourly-2018-3.csv'
        df3 = pd.read_csv(url3)

        url4 = 'http://datamechanics.io/data/ctrinh_fat60221_veeyn/uber-peak-hourly-2018-4.csv'
        df4 = pd.read_csv(url4)

        df1 = df1.append(df2)
        df1 = df1.append(df3)
        df1 = df1.append(df4)

        df = df1.filter(['hod', 'mean_travel_time'])
        r = df.to_dict(orient='records')

        # print(r)
        rdf = pd.DataFrame(r)

        grouped = rdf.groupby(['hod']).agg(sum)

        r = grouped.reset_index().to_dict('records')
        # print(r)

        # print(df2.to_dict(orient='records')[0])
        # print(df2[1:])
        # print(r[-2])

        repo.dropCollection("uber")
        repo.createCollection("uber")
        repo['ctrinh_fat60221_veeyn.uber'].insert_many(r)
        repo['ctrinh_fat60221_veeyn.uber'].metadata({'complete':True})
        print(repo['ctrinh_fat60221_veeyn.uber'].metadata())

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

        this_script = doc.agent('alg:ctrinh_fat60221_veeyn#uber', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dmc:ctrinh_fat60221_veeyn/uber-peak-hourly-2018-$.csv', {'prov:label':'Uber Movement', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_uber = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_uber, this_script)
        doc.usage(get_uber, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        uber = doc.entity('dat:ctrinh_fat60221_veeyn#uber', {prov.model.PROV_LABEL:'Peak Morning Hourly Uber Mean Travel Time', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(uber, this_script)
        doc.wasGeneratedBy(uber, get_uber, endTime)
        doc.wasDerivedFrom(uber, resource, get_uber, get_uber, get_uber)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
uber.execute()
doc = uber.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
