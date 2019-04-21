import dml
from pymongo import MongoClient
import prov
import datetime
import uuid
import urllib.request
import json

class get_schools(dml.Algorithm):
    contributor = 'emilymo'
    reads = []
    writes = ['a.ps', 'a.nps']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0.geojson'
        geturl = urllib.request.urlopen(url).read().decode("utf-8")
        if (trial == True):
            ps = json.loads(geturl)['features'][0:20]
        else:
            ps = json.loads(geturl)['features']
        a['ps'].insert_many(ps)
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1.geojson'
        geturl = urllib.request.urlopen(url).read().decode("utf-8")
        if (trial == True):
            nps = json.loads(geturl)['features'][0:20]
        else:
            nps = json.loads(geturl)['features']
        a['nps'].insert_many(nps)
        
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        doc.add_namespace('bosop', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        
        this_script = doc.agent('alg:emilymo#get_schools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        ps_site = doc.entity('bosop:1d9509a8b2fd485d9ad471ba2fdb1f90_0.geojson', {prov.model.PROV_LABEL:'Boston Public Schools Site', prov.model.PROV_TYPE:'ont:DataResource'})
        nps_site = doc.entity('bosop:0046426a3e4340a6b025ad52b41be70a_1.geojson', {prov.model.PROV_LABEL:'Boston Non-Public Schools Site', prov.model.PROV_TYPE:'ont:DataResource'})
        ps = doc.entity('dat:emilymo#ps', {prov.model.PROV_LABEL:'Boston Public Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        nps = doc.entity('dat:emilymo#nps', {prov.model.PROV_LABEL:'Boston Non-Public Schools', prov.model.PROV_TYPE:'ont:DataSet'})
                         
        doc.wasAssociatedWith(get_schools, this_script)
        doc.wasAttributedTo(ps, this_script)
        doc.wasAttributedTo(nps, this_script)
        doc.wasGeneratedBy(ps, get_schools)
        doc.wasGeneratedBy(nps, get_schools)
        doc.used(get_schools, ps_site, other_attributes={prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.used(get_schools, nps_site, other_attributes={prov.model.PROV_TYPE:'ont:Retrieval'}) 
        doc.wasDerivedFrom(ps, ps_site)
        doc.wasDerivedFrom(nps, nps_site)
        
        return doc
        