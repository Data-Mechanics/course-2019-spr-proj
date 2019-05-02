import dml
from pymongo import MongoClient
import prov
import datetime
import uuid
import bson.code

# get minimum numeric values for reciprocal exchange, social cohesion, and social control within neighborhoods that got consolidated to the same name

class consolidate_survey_neighborhoods(dml.Algorithm):
    contributor = 'emilymo'
    reads = ['a.survtemp']
    writes = ['a.survnew']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
        
        mapper = bson.code.Code("""
                function() {
                    emit(this['NBHDS38_'], {'scch_10':this['scch_10'], 'sccn_10':this['sccn_10'], 'RcpE_10':this['RcpE_10']})
                }""")
        reducer = bson.code.Code("""
                function(key, vs) {
                    var min1 = vs[0]['scch_10'];
                    var min2 = vs[0]['sccn_10'];
                    var min3 = vs[0]['RcpE_10'];
                    vs.forEach(function(v) {
                        if (v['scch_10'] < min1) {min1 = v['scch_10']}
                        if (v['sccn_10'] < min2) {min2 = v['sccn_10']}
                        if (v['RcpE_10'] < min3) {min3 = v['RcpE_10']}
                    })
                    return {'scch_10':min1, 'sccn_10':min2, 'RcpE_10':min3}
                    
                }
                """)
        
        a['survtemp'].map_reduce(mapper, reducer, 'survnew')
        
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
    
    
    
    
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        this_script = doc.agent('alg:emilymo#consolidate_survey_neighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        consolidate_survey_neighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        survtemp = doc.entity('dat:emilymo#survnew', {prov.model.PROV_LABEL:'Survey Data Sorted by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        survnew = doc.entity('dat:emilymo#survnew', {prov.model.PROV_LABEL:'Survey Data Sorted by Neighborhood (Consolidated)', prov.model.PROV_TYPE:'ont:DataSet'})
                             
        doc.wasAssociatedWith(consolidate_survey_neighborhoods, this_script)
        doc.wasAttributedTo(survnew, this_script)
        doc.wasGeneratedBy(survnew, consolidate_survey_neighborhoods)
        doc.used(consolidate_survey_neighborhoods, survtemp, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasDerivedFrom(survnew, survtemp)
        
        return doc
        
        
        
    