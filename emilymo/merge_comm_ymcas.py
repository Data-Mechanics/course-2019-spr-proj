import dml
from pymongo import MongoClient
import prov
import datetime
import uuid

class merge_comm_ymcas(dml.Algorithm):
    contributor = 'emilymo'
    reads = ['a.comm', 'a.ymcas']
    writes = ['a.comy']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
        
        for e in a['comm'].find():
            new = {'name':e['properties']['SITE'], 'source':'comm', 'coordinates':e['geometry']['coordinates']}
            a['comy'].insert_one(new)
        
        for e in a['ymcas'].find():
            newcoords = [e['location'][1], e['location'][0]]
            new = {'name':e['ymca'], 'source':'ymcas', 'coordinates':newcoords}
            a['comy'].insert_one(new)
            
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
    
    
    
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
                
        this_script = doc.agent('alg:emilymo#merge_comm_ymcas', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        merge_comm_ymcas = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        comm = doc.entity('dat:emilymo#comm', {prov.model.PROV_LABEL:'Boston Community Centers', prov.model.PROV_TYPE:'ont:DataSet'})
        ymcas = doc.entity('dat:emilymo#ymcas', {prov.model.PROV_LABEL:'Geocoded YMCAs', prov.model.PROV_TYPE:'ont:DataSet'})
        comy = doc.entity('dat:emilymo#comy', {prov.model.PROV_LABEL:'Community Centers and YMCAs', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAssociatedWith(merge_comm_ymcas, this_script)
        doc.wasAttributedTo(comy, this_script)
        doc.wasGeneratedBy(comy, merge_comm_ymcas)
        doc.used(merge_comm_ymcas, comm, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.used(merge_comm_ymcas, ymcas, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasDerivedFrom(comy, comm)
        doc.wasDerivedFrom(comy, ymcas)
        
        return doc
        
