import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import json

class crashAll(dml.Algorithm):
    contributor = 'robinhe_rqtian_hongyf_zhjiang'
    reads = []
    writes = ['robinhe_rqtian_hongyf_zhjiang.crashAll']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')
        print("database seted up")
        url = 'http://datamechanics.io/data/robinhe_rqtian_hongyf_zhjiang/crash_data_01_19.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        data = json.loads(response)
        # json_file =open('./crash_data_01_19.json')
        # data = json.load(json_file)
        for i in range(0,len(data)):
            del data[i]["_id"]
        print(data[0])
        s = json.dumps(data, sort_keys=True, indent=2)
        repo.dropCollection("crashAll")
        repo.createCollection("crashAll")
        repo['robinhe_rqtian_hongyf_zhjiang.crashAll'].insert_many(data)
        repo['robinhe_rqtian_hongyf_zhjiang.crashAll'].metadata({'complete': True})
        print(repo['robinhe_rqtian_hongyf_zhjiang.crashAll'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:robinhe_rqtian_hongyf_zhjiang#crashAll',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': 'save all data', prov.model.PROV_TYPE: 'ont:DataResource'})
        get_all = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_all, this_script)
        doc.usage(get_all, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )

        crash_data_all = doc.entity('dat:robinhe_rqtian_hongyf_zhjiang#crashAll',
                          {prov.model.PROV_LABEL: 'save all the data', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash_data_all, this_script)
        doc.wasGeneratedBy(crash_data_all, get_all, endTime)
        doc.wasDerivedFrom(crash_data_all, resource, get_all, get_all, get_all)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# crashAll.execute()
# doc = crashAll.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof