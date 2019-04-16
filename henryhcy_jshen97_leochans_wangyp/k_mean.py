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
This scripe is going to implement k means algorithms to find potential locations
that increase the competetiveness of the brand
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
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def dist(p, q):   
    return geodesic(p,q)

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

def algo(n,locations):
    M = []
    
    for i in range(n):
        M.append(locations[i])

    OLD = []
    while OLD != M:
        OLD = M

        MPD = [(m, p, dist(m,p)) for (m, p) in product(M, locations)]
        PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
        PD = aggregate(PDs, min)
        MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MT = aggregate(MP, plus)

        M1 = [(m, 1) for (m, _) in MP]
        MC = aggregate(M1, sum)

        M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]
        M = sorted(M)

    new_stores = dict()
    for i in range(n):
        new_store = dict()
        new_store['lat'] = M[i][0]
        new_store['lng'] = M[i][1]
        new_stores['new store'+str(i)] = new_store

       
    
    return new_stores





class k_mean(dml.Algorithm):
    contributor = 'henryhcy_jshen97_leochans_wangyp'
    reads = ['henryhcy_jshen97_leochans_wangyp.cvs', 'henryhcy_jshen97_leochans_wangyp.walgreen']
    writes = ['henryhcy_jshen97_leochans_wangyp.new_walgreen','henryhcy_jshen97_leochans_wangyp.new_cvs']

    @staticmethod
    def execute(trial = False ):
        startTime = datetime.datetime.now()
        client= dml.pymongo.MongoClient()
        
        repo = client.repo
        repo.authenticate('henryhcy_jshen97_leochans_wangyp','henryhcy_jshen97_leochans_wangyp')
        cvs = repo['henryhcy_jshen97_leochans_wangyp.cvs'].find({})
        walgreen = repo['henryhcy_jshen97_leochans_wangyp.walgreen'].find({})
       
      
        cvs_lcoation = []
        wal_location = []
        for x in cvs:

            cvs_lcoation.append((x['geometry']['location']['lat'], x['geometry']['location']['lng']))

        for x in walgreen:            
            wal_location.append((x['geometry']['location']['lat'], x['geometry']['location']['lng']))

        n = 3
        new_walgreen = algo(n,cvs_lcoation)
        new_cvs = algo(n,wal_location)

        repo.createCollection('new_walgreen')
        repo.createCollection('new_cvs')
        repo['henryhcy_jshen97_leochans_wangyp.new_walgreen'].insert(new_walgreen)
        repo['henryhcy_jshen97_leochans_wangyp.new_walgreen'].metadata({'complete': True})
        repo['henryhcy_jshen97_leochans_wangyp.new_cvs'].insert(new_cvs)
        repo['henryhcy_jshen97_leochans_wangyp.new_cvs'].metadata({'complete': True})

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

        this_script = doc.agent('alg:henryhcy_jshen97_leochans_wangyp#k_mean',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})


        resource1 = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#cvs',
                               {'prov:label': 'cvs', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource2 = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#walgreen',
                               {'prov:label': 'walgreen', prov.model.PROV_TYPE: 'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        output1 = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#new_cvs',
                            {prov.model.PROV_LABEL: 'new_cvs', prov.model.PROV_TYPE: 'ont:DataSet'})
        output2 = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#new_walgreen',
                            {prov.model.PROV_LABEL: 'new_walgreen', prov.model.PROV_TYPE: 'ont:DataSet'})

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

    

        
        
k_mean.execute()
        