import dml
from pymongo import MongoClient
import prov
import datetime
import uuid
import urllib.request
import json

class get_community_centers(dml.Algorithm):
    contributor = 'emilymo'
    reads = []
    writes = ['a.comm']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/acedd06b39854088b0f2fefccffebf84_0.geojson'
        geturl = urllib.request.urlopen(url).read().decode("utf-8")
        
        if (trial == True):
            comm = json.loads(geturl)['features'][0:20]
        else:
            comm = json.loads(geturl)['features']
            
        a['comm'].insert_many(comm)
        
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
    
    
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        doc.add_namespace('bosop', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        
        this_script = doc.agent('alg:emilymo#get_community_centers', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        get_community_centers = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        comm_site = doc.entity('bosop:acedd06b39854088b0f2fefccffebf84_0.geojson', {prov.model.PROV_LABEL:'Boston Community Centers Site', prov.model.PROV_TYPE:'ont:DataResource'})
        comm = doc.entity('dat:emilymo#comm', {prov.model.PROV_LABEL:'Boston Community Centers', prov.model.PROV_TYPE:'ont:DataSet'})
                          
        doc.wasAssociatedWith(get_community_centers, this_script)
        doc.wasAttributedTo(comm, this_script)
        doc.wasGeneratedBy(comm, get_community_centers)
        doc.used(get_community_centers, comm_site, other_attributes={prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasDerivedFrom(comm, comm_site)
        
        return doc
