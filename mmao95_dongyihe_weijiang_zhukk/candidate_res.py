import urllib.request
import json
import dml
import pymongo
import prov.model
import datetime
import uuid
import pandas as pd
from random import shuffle
from math import sqrt

class candidate_res(dml.Algorithm):
    contributor = 'mmao95_Dongyihe_weijiang_zhukk'
    reads = [contributor + '.streetbook_alternate',contributor + '.streetbook_filtered',contributor + '.cau_landmark_merge']
    writes = [contributor + '.candidate_res']
    @staticmethod
    def execute(trial=False):
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
            return [(t, u) for t in R for u in S]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k, v) in R if k == key])) for key in keys]

        def avg(x): # Average
            return sum(x)/len(x)

        def stddev(x): # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

        def cov(x, y): # Covariance.
            return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

        def corr(x, y): # Correlation coefficient.
            if stddev(x)*stddev(y) != 0:
                return cov(x, y)/(stddev(x)*stddev(y))
            
        start_time = datetime.datetime.now()
        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        reads = [contributor + '.streetbook_alternate',contributor + '.streetbook_filtered',contributor + '.cau_landmark_merge']
        writes = [contributor + '.candidate_res']
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)
        rec = repo[reads[0]]
        streetbook_filtered = repo[reads[1]]
        landmark = repo[reads[2]]
        bt = list(rec.find())
        sf = list(streetbook_filtered.find())
        lm = list(landmark.find())
        l1 = project(sf,lambda t:[t['FullName'],t['StreetName'],t['Zipcode'],0,0])
        mapp = {"Fenway/Kenmore":"02215", "Charlestown":"02129","North End":"02110","West End":"02114",
                "South End":"02116","Boston":"02108","Roxbury":"02120","North Dorchester":"02125",
                "Allston/Brighton":"02134","Chinatown":"02111","Financial District":"02110",
                "Back Bay":"02116","Beacon Hill":"02116","South Dorchester":"02124",
                "Jamaica Plain":"02467","West Roxbury":"02132","Hyde Park":"02136","South Cove":"02116",
                "Dorchester":"02121","East Boston":"02128","Brighton":"02135","Chestnut Hill":"02467",
                "South Boston":"02127","Theater":"02116","Fenway, JP":"02445","Theater District":"02111",
                "Mission Hill":"02120","Beacon Hill/Back Bay":"02116","Mattapan":"02126","Allston":"02134","Fenway":"02215"}
        for ll in l1:
            for k in bt:
                if ll[1]==k['StreetName']: 
                    ll[3]=k['RedundantTime']
            for j in lm:
                if j['Neighborhood'] and mapp[j['Neighborhood']]==ll[2]:
                    ll[4]+=1
                    
        for i in l1:
            if i[3]==0 and i[4]==0:
                l1.remove(i)
        x = [x[3] for x in l1]
        y = [x[4] for x in l1]
        print(corr(x,y))
        repo.dropCollection('candidate_res')
        repo.createCollection('candidate_res')
        columnName = ['FullName','StreetName','Zipcode','Duplicate','Landmark']
        df = pd.DataFrame(columns=columnName,data=l1)
        idx=df.groupby(by='Zipcode')['Duplicate'].idxmax()
        res=df.loc[idx,]
        data = json.loads(res.to_json(orient="records"))
        repo[writes[0]].insert_many(data)
        repo[writes[0]].metadata({'complete': True})
        repo.logout()
        endtime = datetime.datetime.now()
        return {"start":start_time, "end":endtime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        
        this_script = doc.agent('alg:' + contributor + '#uber_data',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        
        get_names = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_names, this_script)
        doc.usage(get_names, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Computation': 'Data cleaning'
                   }
                  )

        fp = doc.entity('dat:' + contributor + '#streetbook_alternate',
                        {prov.model.PROV_LABEL: 'Streetbook Alternate', prov.model.PROV_TYPE: 'ont:DataSet'})
        fp1 = doc.entity('dat:' + contributor + '#streetbook_filtered',
                        {prov.model.PROV_LABEL: 'Streetbook Filtered', prov.model.PROV_TYPE: 'ont:DataSet'})
        fp2 = doc.entity('dat:' + contributor + '#cau_landmark_merge',
                        {prov.model.PROV_LABEL: 'Cau Landmark Merge', prov.model.PROV_TYPE: 'ont:DataSet'})
        r1 = doc.entity('dat:' + contributor + '#candidate_res',
                        {prov.model.PROV_LABEL: 'Candidate Res', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fp, this_script)
        doc.wasAttributedTo(fp1, this_script)
        doc.wasAttributedTo(fp2, this_script)
        doc.wasGeneratedBy(r1, get_names, endTime)
        doc.wasDerivedFrom(r1, fp, get_names, get_names, get_names)
        doc.wasDerivedFrom(r1, fp1, get_names, get_names, get_names)
        doc.wasDerivedFrom(r1, fp2, get_names, get_names, get_names)

        repo.logout()

        return doc
