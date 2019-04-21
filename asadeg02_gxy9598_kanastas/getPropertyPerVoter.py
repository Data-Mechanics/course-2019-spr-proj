from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid


class getPropertyPerVoter(dml.Algorithm):

    contributor = 'asadeg02_gxy9598'
    reads = []
    writes = ['asadeg02_gxy9598.properties_per_voter']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')  
        
        url = 'http://datamechanics.io/data/asadeg02/Property-Per-Voter.json'
        response = urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        
        repo.dropCollection('asadeg02_gxy9598.properties_per_voter')
        repo.createCollection('asadeg02_gxy9598.properties_per_voter')

        #repo["asadeg02_gxy9598.properties-per-voter"].insert_many(prop_per_voter)
        _id = 0
        for doc in r:
            doc['_id'] = _id
            repo['asadeg02_gxy9598.properties_per_voter'].insert(doc)
            _id += 1

        repo["asadeg02_gxy9598.properties_per_voter"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.properties_per_voter"].metadata())
        print('Finished Loading Property Per Voter File')

        repo.logout()
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):     
        

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:asadeg02_gxy9598#getPropertyPerVoter', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})        
        get_property_per_voter = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Properties Per Voter File', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_property_per_voter, this_script)
        
        resource_prop_per_voter = doc.entity('dat:asadeg02', {'prov:label':'Properties Per Voter File', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        properties_per_voter = doc.entity('dat:asadeg02_gxy9598#properties-per-voter', {'prov:label':'Properties Per Voter ', prov.model.PROV_TYPE:'ont:Dataset'})        
        doc.usage(get_property_per_voter, resource_prop_per_voter, startTime)

        
        doc.wasAttributedTo(get_property_per_voter, this_script)
        doc.wasGeneratedBy(properties_per_voter, get_property_per_voter, endTime)
        doc.wasDerivedFrom(properties_per_voter, resource_prop_per_voter, get_property_per_voter, get_property_per_voter, get_property_per_voter)
        
        
        return doc
