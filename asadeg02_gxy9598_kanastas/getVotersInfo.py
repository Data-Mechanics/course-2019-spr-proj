from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid


class getVotersInfo(dml.Algorithm):

    contributor = 'asadeg02_gxy9598'
    reads = []
    writes = ['asadeg02_gxy9598.voters_info']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')  
        
        url = 'http://datamechanics.io/data/asadeg02/Voter-File.json'
        response = urlopen(url).read().decode("utf-8")
        r = json.loads(response)        
        #voter_info = json.dumps(r, sort_keys=True, indent=2)     
        
        
        repo.dropCollection('asadeg02_gxy9598.voters_info')
        repo.createCollection('asadeg02_gxy9598.voters_info')

        #repo["asadeg02_gxy9598.voters_info"].insert_many(voter_info)
        _id = 0
        for doc in r:
            doc['_id'] = _id
            repo['asadeg02_gxy9598.voters_info'].insert(doc)
            _id += 1

        repo["asadeg02_gxy9598.voters_info"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.voters_info"].metadata())
        print('Finished Loading Voter File')

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

        this_script = doc.agent('alg:asadeg02_gxy9598#getVotersInfo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})        
        get_voters_info = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Voters Information In South Boston', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_voters_info, this_script)
        
        resource_voter_file = doc.entity('dat:asadeg02', {'prov:label':'South Bsoton Voter File', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        voters_info = doc.entity('dat:asadeg02_gxy9598#voters_info', {'prov:label':'South Boston Voters Info', prov.model.PROV_TYPE:'ont:Dataset'})        
        doc.usage(get_voters_info, resource_voter_file, startTime)

        
        doc.wasAttributedTo(get_voters_info, this_script)
        doc.wasGeneratedBy(voters_info, get_voters_info, endTime)
        doc.wasDerivedFrom(voters_info, resource_voter_file, get_voters_info, get_voters_info, get_voters_info)
        
        
        return doc