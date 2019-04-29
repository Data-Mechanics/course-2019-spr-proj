import dml
from pymongo import MongoClient
import prov
import datetime
import uuid
import bson.code
from shapely.geometry.polygon import Polygon
from shapely.geometry import Point

class sort(dml.Algorithm):
    contributor = 'emilymo'
    reads = ['a.nbhdtemp', 'a.comy', 'a.ps', 'a.nps']
    writes = ['a.cmpresort', 'a.cmsort', 'a.pspresort', 'a.pssort', 'a.npspresort', 'a.npssort']
    
    # the following MongoDB mapreduce code is returning NaN values in the emit portion, resulting in false counts. To circumvent this problem, I made a new set in Python that performs the computations correctly, then inserted it into MongoDB. See the actual 'execute' method below this one.
    '''
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a

        ### sort community centers / ymcas into neighborhoods
        cmpresort = []
        for nb in a['nbhdtemp'].find():
            n = nb['coordinates']
            name = nb['Name']
            
            for pc in n: # for each neighborhood piece
                if (len(n) > 1):
                    if len(n[0]) > 1: # if not actually double nested
                        p = pc
                    else:
                        p = pc[0]
                else:
                    p = pc
                    
                poly = Polygon(p) 
                
                for c in a['comy'].find(): # for each community center / ymca
                    
                    cname = c['name']
                    
                    coords = Point(tuple(c['coordinates']))
                    b = poly.contains(coords)
                    
                    entry = {'nbhd':name, 'comm':cname, 'in':int(b)}
                    cmpresort.append(entry)
        a['cmpresort'].insert_many(cmpresort)
        
        mapper = bson.code.Code("""
                    function () {
                        emit(
                            this['nbhd'], {'comm':this['comm'], 'in':this['in']}
                        )
                    }""")
        reducer = bson.code.Code("""
                    function (key, vs) {
                        var tot = 0;
                        vs.forEach(function(v) {
                            var num = Number(v['in']);
                            if (isNaN(num)) {
                            tot += 0} else {
                            tot += num}
                        });
                    return {'comms':tot};
                    }""")
        a['cmpresort'].map_reduce(mapper, reducer, 'cmsort')
        
        
        ### sort public schools into neighborhoods
        pspresort = []
        for nb in a['nbhdtemp'].find():
            n = nb['coordinates']
            name = nb['Name']
            
            for pc in n: # for each neighborhood piece
                if (len(n) > 1):
                    if len(n[0]) > 1: # if not actually double nested
                        p = pc
                    else:
                        p = pc[0]
                else:
                    p = pc
                    
                poly = Polygon(p) 
                
                for s in a['ps'].find(): # for each school
                    
                    sname = s['properties']['SCH_NAME']
                    
                    coords = Point(tuple(s['geometry']['coordinates']))
                    b = poly.contains(coords)
                    
                    entry = {'nbhd':name, 'pschool':sname, 'in':int(b)}
                    pspresort.append(entry)
        a['pspresort'].insert_many(pspresort)
        
        mapper2 = bson.code.Code("""
                    function () {
                        emit(
                            this['nbhd'], {'pschool':this['pschool'], 'in':this['in']}
                        )
                    }""")
        reducer2 = bson.code.Code("""
                    function (key, vs) {
                        var tot = 0;
                        vs.forEach(function(v) {
                            var num = Number(v['in']);
                            if (isNaN(num)) {
                            tot += 0} else {
                            tot += num}
                        });
                    return {'pss':tot};
                    }""")
        a['pspresort'].map_reduce(mapper2, reducer2, 'pssort')
        
        
        ### sort non-public schools into neighborhoods
        npspresort = []
        for nb in a['nbhdtemp'].find():
            n = nb['coordinates']
            name = nb['Name']
            
            for pc in n: # for each neighborhood piece
                if (len(n) > 1):
                    if len(n[0]) > 1: # if not actually double nested
                        p = pc
                    else:
                        p = pc[0]
                else:
                    p = pc
                    
                poly = Polygon(p) 
                
                for s in a['nps'].find(): # for each school
                    
                    sname = s['properties']['NAME']
                    
                    coords = Point(tuple(s['geometry']['coordinates']))
                    b = poly.contains(coords)
                    
                    entry = {'nbhd':name, 'pschool':sname, 'in':int(b)}
                    npspresort.append(entry)
        a['npspresort'].insert_many(npspresort)
        
        mapper3 = bson.code.Code("""
                    function () {
                        emit(
                            this['nbhd'], {'npschool':this['npschool'], 'in':this['in']}
                        )
                    }""")
        reducer3 = bson.code.Code("""
                    function (key, vs) {
                        var tot = 0;
                        vs.forEach(function(v) {
                            var num = Number(v['in']);
                            if (isNaN(num)) {
                            tot += 0} else {
                            tot += num}
                        });
                    return {'npss':tot};
                    }""")
        a['npspresort'].map_reduce(mapper3, reducer3, 'npssort')
            
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
    '''
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a

        ### sort community centers / ymcas into neighborhoods
        cmpresort = []
        for nb in a['nbhdtemp'].find():
            n = nb['coordinates']
            name = nb['Name']
            
            for pc in n: # for each neighborhood piece
                if (len(n) > 1):
                    if len(n[0]) > 1: # if not actually double nested
                        p = pc
                    else:
                        p = pc[0]
                else:
                    p = pc
                    
                poly = Polygon(p) 
                
                for c in a['comy'].find(): # for each community center / ymca
                    
                    cname = c['name']
                    
                    coords = Point(tuple(c['coordinates']))
                    b = poly.contains(coords)
                    
                    entry = {'nbhd':name, 'comm':cname, 'in':int(b)}
                    cmpresort.append(entry)
        a['cmpresort'].insert_many(cmpresort)
        mapped1 = []
        for e in cmpresort:
            emit = (e['nbhd'], {'comm':e['comm'], 'in':e['in']})
            mapped1.append(emit)
        
        keys = list({k for k, v in mapped1})
        cmsort = []
        for k in keys:
            ins = [e[1]['in'] for e in mapped1 if e[0] == k and e[1]['in'] == 1]
            insum = sum(ins)
            t = ({'_id':k, 'value':{'comms':insum}}) 
            cmsort.append(t)
        a['cmsort'].insert_many(cmsort)
            
        ### sort public schools into neighborhoods
        pspresort = []
        for nb in a['nbhdtemp'].find():
            n = nb['coordinates']
            name = nb['Name']
            
            for pc in n: # for each neighborhood piece
                if (len(n) > 1):
                    if len(n[0]) > 1: # if not actually double nested
                        p = pc
                    else:
                        p = pc[0]
                else:
                    p = pc
                    
                poly = Polygon(p) 
                
                for s in a['ps'].find(): # for each school
                    
                    sname = s['properties']['SCH_NAME']
                    
                    coords = Point(tuple(s['geometry']['coordinates']))
                    b = poly.contains(coords)
                    
                    entry = {'nbhd':name, 'pschool':sname, 'in':int(b)}
                    pspresort.append(entry)
        a['pspresort'].insert_many(pspresort)
        mapped2 = []
        for e in pspresort:
            emit = (e['nbhd'], {'pschool':e['pschool'], 'in':e['in']})
            mapped2.append(emit)
        
        keys2 = list({k for k, v in mapped2})
        pssort = []
        for k in keys2:
            ins = [e[1]['in'] for e in mapped2 if e[0] == k and e[1]['in'] == 1]
            insum = sum(ins)
            t = ({'_id':k, 'value':{'pss':insum}}) 
            pssort.append(t)
        a['pssort'].insert_many(pssort)
        
        ### sort non-public schools into neighborhoods
        npspresort = []
        for nb in a['nbhdtemp'].find():
            n = nb['coordinates']
            name = nb['Name']
            
            for pc in n: # for each neighborhood piece
                if (len(n) > 1):
                    if len(n[0]) > 1: # if not actually double nested
                        p = pc
                    else:
                        p = pc[0]
                else:
                    p = pc
                    
                poly = Polygon(p) 
                
                for s in a['nps'].find(): # for each school
                    
                    sname = s['properties']['NAME']
                    
                    coords = Point(tuple(s['geometry']['coordinates']))
                    b = poly.contains(coords)
                    
                    entry = {'nbhd':name, 'npschool':sname, 'in':int(b)}
                    npspresort.append(entry)
        a['npspresort'].insert_many(npspresort)
        mapped3 = []
        for e in npspresort:
            emit = (e['nbhd'], {'npschool':e['npschool'], 'in':e['in']})
            mapped3.append(emit)
        
        keys3 = list({k for k, v in mapped3})
        npssort = []
        for k in keys3:
            ins = [e[1]['in'] for e in mapped3 if e[0] == k and e[1]['in'] == 1]
            insum = sum(ins)
            t = ({'_id':k, 'value':{'npss':insum}}) 
            npssort.append(t)
        a['npssort'].insert_many(npssort)
        
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
            
    
    
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        this_script = doc.agent('alg:emilymo#sort', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        sort = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        nbhdtemp = doc.entity('dat:emilymo#nbhdtemp', {prov.model.PROV_LABEL:'Boston Neighborhoods Regrouped', prov.model.PROV_TYPE:'ont:DataSet'})
        comy = doc.entity('dat:emilymo#comy', {prov.model.PROV_LABEL:'Community Centers and YMCAs', prov.model.PROV_TYPE:'ont:DataSet'})
        ps = doc.entity('dat:emilymo#ps', {prov.model.PROV_LABEL:'Boston Public Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        nps = doc.entity('dat:emilymo#nps', {prov.model.PROV_LABEL:'Boston Non-Public Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        cmpresort = doc.entity('dat:emilymo#cmpresort', {prov.model.PROV_LABEL:'Community Centers Pre-Sort Format', prov.model.PROV_TYPE:'ont:DataSet'})
        cmsort = doc.entity('dat:emilymo#cmsort', {prov.model.PROV_LABEL:'Community Centers Sorted by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        pspresort = doc.entity('dat:emilymo#pspresort', {prov.model.PROV_LABEL:'Public Schools Pre-Sort Format', prov.model.PROV_TYPE:'ont:DataSet'})
        pssort = doc.entity('dat:emilymo#pssort', {prov.model.PROV_LABEL:'Public Schools Sorted by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        npspresort = doc.entity('dat:emilymo#npspresort', {prov.model.PROV_LABEL:'Non-Public Schools Pre-Sort Format', prov.model.PROV_TYPE:'ont:DataSet'})
        npssort = doc.entity('dat:emilymo#npssort', {prov.model.PROV_LABEL:'Non-Public Schools Sorted by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
                             
        doc.wasAssociatedWith(sort, this_script)
        doc.wasAttributedTo(cmpresort, this_script)
        doc.wasAttributedTo(cmsort, this_script)
        doc.wasAttributedTo(pspresort, this_script)
        doc.wasAttributedTo(pssort, this_script)
        doc.wasAttributedTo(npspresort, this_script)
        doc.wasAttributedTo(npssort, this_script)
        doc.wasGeneratedBy(cmpresort, sort)
        doc.wasGeneratedBy(cmsort, sort)
        doc.wasGeneratedBy(pspresort, sort)
        doc.wasGeneratedBy(pssort, sort)
        doc.wasGeneratedBy(npspresort, sort)
        doc.wasGeneratedBy(npssort, sort)
        doc.used(sort, nbhdtemp)
        doc.used(sort, comy)
        doc.used(sort, ps)
        doc.used(sort, nps)
        doc.used(sort, cmpresort)
        doc.used(sort, pspresort)
        doc.used(sort, npspresort)
        doc.wasDerivedFrom(cmpresort, nbhdtemp)
        doc.wasDerivedFrom(cmpresort, comy)
        doc.wasDerivedFrom(pspresort, nbhdtemp)
        doc.wasDerivedFrom(pspresort, ps)
        doc.wasDerivedFrom(npspresort, nbhdtemp)
        doc.wasDerivedFrom(npspresort, nps)
        doc.wasDerivedFrom(cmsort, cmpresort)
        doc.wasDerivedFrom(pssort, pspresort)
        doc.wasDerivedFrom(npssort, npspresort)
        
        return doc
                 