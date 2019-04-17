import dml
import datetime
import json
import prov.model
import pandas as pd
import pprint
import uuid
from urllib.request import urlopen
import math
from geopy.distance import geodesic

'''
This scripe is going to read two collections, for every cvs store, we are going to find
the closest walgreen and 7/11 store and calculate the distance. And save it into the data base
'''

def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,_,v) in R if k == key])) for key in keys]

class find_Distance(dml.Algorithm):
    contributor = 'henryhcy_jshen97_leochans_wangyp'
    reads = ['henryhcy_jshen97_leochans_wangyp.cvs', 'henryhcy_jshen97_leochans_wangyp.walgreen']
    writes = ['henryhcy_jshen97_leochans_wangyp.wal_wal_cvs','henryhcy_jshen97_leochans_wangyp.cvs_wal_cvs']

    @staticmethod
    def execute(trial = False ):
        startTime = datetime.datetime.now()
        client= dml.pymongo.MongoClient()
        
        repo = client.repo
        repo.authenticate('henryhcy_jshen97_leochans_wangyp','henryhcy_jshen97_leochans_wangyp')
        cvs = repo['henryhcy_jshen97_leochans_wangyp.cvs'].find({})
        walgreen = repo['henryhcy_jshen97_leochans_wangyp.walgreen'].find({})
        newCVS = []
        newWalgreen = []
      
        for x in cvs:
            s = dict()
            s['id'] = x['place_id']
            s['location']= [x['geometry']['location']['lat'], x['geometry']['location']['lng']]
            newCVS.append(s)

        for x in walgreen:
            s = dict()
            s['id'] = x['place_id']
            s['location']= [x['geometry']['location']['lat'], x['geometry']['location']['lng']]
            newWalgreen.append(s)

        cvsWal = product(newCVS,newWalgreen)
        cvsCvs = product(newCVS,newCVS)
        temp1 = project(cvsCvs,lambda t1:(t1[0]['id'], t1[1]['id'],geodesic((t1[0]['location'][0],t1[0]['location'][1]), (t1[1]['location'][0],t1[1]['location'][1])).miles))
        temp1 = select(temp1,lambda t1: t1[0] != t1[1])
        temp2 = aggregate(temp1,min)
        temp3 = select(product(temp1,temp2), lambda t: t[0][2] == t[1][1] and t[0][0] == t[1][0])
        cvsCvs_min_dis = project(temp3,lambda t: t[0])

        cvsRel = dict()
        for i in cvsCvs_min_dis:
            closest_cvs = dict()
            closest_cvs['cvs'] = [i[1],i[2]]
            cvsRel[i[0]] = closest_cvs
        
        temp1 = project(cvsWal,lambda t1:(t1[0]['id'], t1[1]['id'],geodesic((t1[0]['location'][0],t1[0]['location'][1]), (t1[1]['location'][0],t1[1]['location'][1])).miles))
        temp2 = aggregate(temp1,min)
        temp3 = select(product(temp1,temp2), lambda t: t[0][2] == t[1][1] and t[0][0] == t[1][0])
        cvsWal_min_dis = project(temp3,lambda t: t[0])

        for i in cvsWal_min_dis:
            cvsRel[i[0]]['walgreen'] = [i[1],i[2]]
        

    
        walWal = product(newWalgreen,newWalgreen)
        temp1 = project(walWal,lambda t1:(t1[0]['id'], t1[1]['id'],geodesic((t1[0]['location'][0],t1[0]['location'][1]), (t1[1]['location'][0],t1[1]['location'][1])).miles))
        temp1 = select(temp1,lambda t1: t1[0] != t1[1])
        temp2 = aggregate(temp1,min)
        temp3 = select(product(temp1,temp2), lambda t: t[0][2] == t[1][1] and t[0][0] == t[1][0])
        walWal_min_dis = project(temp3,lambda t: t[0])

        walRel = dict()
        for i in walWal_min_dis:
            wal = dict()
            wal['walgreen'] = [i[1],i[2]]
            walRel[i[0]] = wal
            
        
        
        
        walCVS = product(newWalgreen,newCVS)
        temp1 = project(walCVS,lambda t1:(t1[0]['id'], t1[1]['id'],geodesic((t1[0]['location'][0],t1[0]['location'][1]), (t1[1]['location'][0],t1[1]['location'][1])).miles))
        temp2 = aggregate(temp1,min)
        temp3 = select(product(temp1,temp2), lambda t: t[0][2] == t[1][1] and t[0][0] == t[1][0])
        walCVS_min_dis = project(temp3,lambda t: t[0])

        for i in walCVS_min_dis:
            
            walRel[i[0]]['cvs'] = [i[1],i[2]]
        
        waltocvs =[]
        waltowal =[]

        cvstowal = []
        cvstocvs = []

        for key,values in walRel.items():
            waltocvs.append(values['cvs'][1])
            waltowal.append(values['walgreen'][1])
        for key,values in cvsRel.items():
            cvstocvs.append(values['cvs'][1])
            cvstowal.append(values['walgreen'][1])
        repo.dropCollection('wal_wal&cvs')
        repo.dropCollection('cvs_wal&cvs')

    
        cvstocvs_mean = sum(cvstocvs)/len(cvstocvs)
        cvstowal_mean = sum(cvstowal)/len(cvstowal)
        waltocvs_mean = sum(waltocvs)/len(waltocvs)
        waltowal_mean = sum(waltowal)/len(waltowal)

        cvstocvs_stdv = math.sqrt(sum([(xi-cvstocvs_mean)**2 for xi in cvstocvs])/len(cvstocvs))
        cvstowal_stdv = math.sqrt(sum([(xi-cvstowal_mean)**2 for xi in cvstowal])/len(cvstowal))
        waltocvs_stdv = math.sqrt(sum([(xi-waltocvs_mean)**2 for xi in waltocvs])/len(waltocvs))
        waltowal_stdv = math.sqrt(sum([(xi-waltowal_mean)**2 for xi in waltowal])/len(waltowal))

        #print(sum([1 for x in cvstocvs if x == 0]))
     
        #print(sum([1 for x in cvstowal if x == 0]))

        repo.createCollection('wal_wal_cvs')
        repo.createCollection('cvs_wal_cvs')
        repo['henryhcy_jshen97_leochans_wangyp.wal_wal_cvs'].insert(walRel)
        repo['henryhcy_jshen97_leochans_wangyp.wal_wal_cvs'].metadata({'complete': True})
        repo['henryhcy_jshen97_leochans_wangyp.cvs_wal_cvs'].insert(cvsRel)
        repo['henryhcy_jshen97_leochans_wangyp.cvs_wal_cvs'].metadata({'complete': True})
        

        repo.logout()

        end_time = datetime.datetime.now()

        return {"start": startTime, "end": end_time}
    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.'''
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/767/71c/')

        this_script = doc.agent('alg:henryhcy_jshen97_leochans_wangyp#find_Distance',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})


        resource1 = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#cvs',
                               {'prov:label': 'cvs', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource2 = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#walgreen',
                               {'prov:label': 'walgreen', prov.model.PROV_TYPE: 'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        output1 = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#wal_wal&cvs',
                            {prov.model.PROV_LABEL: 'cvs_wal&cvs', prov.model.PROV_TYPE: 'ont:DataSet'})
        output2 = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#wal_wal&cvs',
                            {prov.model.PROV_LABEL: 'wal_wal&cvs', prov.model.PROV_TYPE: 'ont:DataSet'})

        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource1, startTime)
        doc.used(this_run, resource2, startTime)


        doc.wasAttributedTo(output1, this_script)
        doc.wasAttributedTo(output2, this_script)

        doc.wasGeneratedBy(output1, this_run, endTime)
        doc.wasGeneratedBy(output2, this_run, endTime)
        doc.wasDerivedFrom(output1, resource1, this_run, this_run, this_run)
        doc.wasDerivedFrom(output1, resource2, this_run, this_run, this_run)
        doc.wasDerivedFrom(output2, resource1, this_run, this_run, this_run)
        doc.wasDerivedFrom(output2, resource2, this_run, this_run, this_run)

        repo.logout()

        return doc

'''
find_Distance.execute()
doc = find_Distance.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''