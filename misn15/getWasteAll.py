import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class getWasteAll(dml.Algorithm):
    contributor = 'misn15'
    reads = []
    writes = ['misn15.hwgen', 'misn15.aul', 'misn15.waste']

    @staticmethod
    def execute(trial = False):
        '''Retrieve waste data from datamechanics.io'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        # source one
        url = 'http://datamechanics.io/data/hwgenids.json' 
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hwgen")
        repo.createCollection("hwgen")
        repo['misn15.hwgen'].insert_many(r)
        repo['misn15.hwgen'].metadata({'complete':True})
        print(repo['misn15.hwgen'].metadata())

        # source two
        url = 'http://datamechanics.io/data/misn15/master_waste/AUL_PT.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("aul")
        repo.createCollection("aul")
        repo['misn15.aul'].insert_many(r['features'])
        repo['misn15.aul'].metadata({'complete':True})
        print(repo['misn15.aul'].metadata())

        # source three
        url = 'http://datamechanics.io/data/misn15/master_waste/c21e.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("waste")
        repo.createCollection("waste")
        repo['misn15.waste'].insert_many(r['features'])
        repo['misn15.waste'].metadata({'complete':True})
        print(repo['misn15.waste'].metadata())

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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://docs.digital.mass.gov/dataset/')

        this_script = doc.agent('alg:misn15#getWasteAll', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:list-massachusetts-hazardous-waste-generators-january-23-2018', {'prov:label':'Boston_waste', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('bdp:massgis-data-massdep-oil-andor-hazardous-material-sites-activity-and-use-limitations-aul', {'prov:label': 'Boston_waste', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'geojson'})
        resource3 = doc.entity('bdp:massgis-data-massdep-tier-classified-oil-andor-hazardous-material-sites-mgl-c-21e', {'prov:label': 'Boston_waste', prov.model.PROV_TYPE: 'ont:DataResource','ont:Extension': 'geojson'})
        get_hwgen = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hwgen, this_script)
        doc.usage(get_hwgen, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                   }
                  )

        get_aul = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aul, this_script)
        doc.usage(get_aul, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                   }
                  )

        get_waste = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_waste, this_script)
        doc.usage(get_waste, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                   }
                  )

        hwgen = doc.entity('dat:misn15#hwgen', {prov.model.PROV_LABEL:'Boston Hazardous Waste/Oil', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hwgen, this_script)
        doc.wasGeneratedBy(hwgen, get_hwgen, endTime)
        doc.wasDerivedFrom(hwgen, resource, get_hwgen, get_hwgen, get_hwgen)

        aul = doc.entity('dat:misn15#aul', {prov.model.PROV_LABEL: 'Boston Waste with Limited Use', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(aul, this_script)
        doc.wasGeneratedBy(aul, get_aul, endTime)
        doc.wasDerivedFrom(aul, resource2, get_aul, get_aul, get_aul)

        waste = doc.entity('dat:misn15#waste', {prov.model.PROV_LABEL: 'Boston Hazardous Waste', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(waste, this_script)
        doc.wasGeneratedBy(waste, get_waste, endTime)
        doc.wasDerivedFrom(waste, resource3, get_waste, get_waste, get_waste)
                  
        return doc

##getWasteAll.execute()
##doc = getWasteAll.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
