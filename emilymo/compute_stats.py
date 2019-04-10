import dml
from pymongo import MongoClient
import prov
import datetime
import uuid

class compute_stats(dml.Algorithm):
    contributor = 'emilymo'
    reads = ['a.info']
    writes = ['a.stat', 'a.bnd']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
        
        for i in ['cohesion', 'control', 'reciprocal']:
            var = i
            mean = sum([e[i] for e in a['info'].find()]) / len([e for e in a['info'].find()])
            sd = sum([(e[i]-mean)**2 for e in a['info'].find()]) / len([e for e in a['info'].find()])
            t = {'var':var, 'mean':mean, 'sd':sd}
            a['stat'].insert_one(t)
            
        # means for each survey var, which will become bounds for constraints
        bnd = {}
        for r in a['stat'].find():
            var = r['var']
            val = r['mean']
            bnd.update({var:val})
        a['bnd'].insert_one(bnd)
        
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
    
    
    
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        this_script = doc.agent('alg:emilymo#compute_stats', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        compute_stats = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        info = doc.entity('dat:emilymo#info', {prov.model.PROV_LABEL:'Final Data per Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        stat = doc.entity('dat:emilymo#stat', {prov.model.PROV_LABEL:'Statistics', prov.model.PROV_TYPE:'ont:DataSet'})
        bnd = doc.entity('dat:emilymo#bnd', {prov.model.PROV_LABEL:'Mean Survey Values Across All Neighborhoods', prov.model.PROV_TYPE:'ont:DataSet'})
                         
        doc.wasAssociatedWith(compute_stats, this_script)
        doc.wasAttributedTo(stat, this_script)
        doc.wasAttributedTo(bnd, this_script)
        doc.wasGeneratedBy(stat, compute_stats)
        doc.wasGeneratedBy(bnd, compute_stats)
        doc.used(compute_stats, info, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasDerivedFrom(stat, info)
        doc.wasDerivedFrom(bnd, stat)
        
        return doc
        