import dml
from pymongo import MongoClient
import prov
import datetime
import uuid

class join_sorted_data(dml.Algorithm):
    contributor = 'emilymo'
    reads = ['a.cmsort', 'a.pssort', 'a.npssort', 'a.survnew']
    writes = ['a.info']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
    
        for n in list(set([n['Name'] for n in a['nbhdtemp'].find()])):
            for c in a['cmsort'].find():
                if n == c['_id']:
                    comms = c['value']['comms']
            for ps in a['pssort'].find():
                if n == ps['_id']:
                    pss = ps['value']['pss']
            for nps in a['npssort'].find():
                if n == nps['_id']:
                    npss = nps['value']['npss']
            for s in a['survnew'].find():
                if n == s['_id']:
                    cohesion = s['value']['scch_10']
                    control = s['value']['sccn_10']
                    reciprocal = s['value']['RcpE_10']
            r = {'nbhd':n, 'comms':comms, 'pss':pss, 'npss':npss, 'cohesion':cohesion, 'control':control, 'reciprocal':reciprocal}
            a['info'].insert_one(r)
        
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        this_script = doc.agent('alg:emilymo#join_sorted_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        join_sorted_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        cmsort = doc.entity('dat:emilymo#cmsort', {prov.model.PROV_LABEL:'Community Centers Sorted by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        pssort = doc.entity('dat:emilymo#pssort', {prov.model.PROV_LABEL:'Public Schools Sorted by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        npssort = doc.entity('dat:emilymo#npssort', {prov.model.PROV_LABEL:'Non-Public Schools Sorted by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        survnew = doc.entity('dat:emilymo#survnew', {prov.model.PROV_LABEL:'Survey Data Sorted by Neighborhood (Consolidated)', prov.model.PROV_TYPE:'ont:DataSet'})
        info = doc.entity('dat:emilymo#info', {prov.model.PROV_LABEL:'Community Centers, Schools, and Survey Data by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAssociatedWith(join_sorted_data, this_script)
        doc.wasAttributedTo(info, this_script)
        doc.wasGeneratedBy(info, join_sorted_data)
        doc.used(join_sorted_data, cmsort, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.used(join_sorted_data, pssort, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.used(join_sorted_data, npssort, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.used(join_sorted_data, survnew, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasDerivedFrom(info, cmsort)
        doc.wasDerivedFrom(info, pssort)
        doc.wasDerivedFrom(info, npssort)
        doc.wasDerivedFrom(info, survnew)
        
        return doc


                          
        
