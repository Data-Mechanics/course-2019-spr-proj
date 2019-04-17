import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io
import numpy as np

class analysis(dml.Algorithm):
    contributor = 'ruipang_zhou482'
    reads = ['ruipang_zhou482.totalSchool', 'ruipang_zhou482.hospital','ruipang_zhou482.propertyAssessment','ruipang_zhou482.police']
    writes = []
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ruipang_zhou482', 'ruipang_zhou482')

        hospital=[]
        school=[]
        prop=[]
        police = []
        print("check")
        for i in repo['ruipang_zhou482.totalSchool'].find():
            
            school.append(i)
        for i in repo['ruipang_zhou482.police'].find():
            
            police.append(i)
        for i in repo['ruipang_zhou482.hospital'].find():
            hospital.append(i)
        for i in repo['ruipang_zhou482.propertyAssessment'].find():
            
            prop.append(i)
        police_mat = analysis.coe(prop,police)
        police_cov = 0
        school_mat = analysis.coe(prop,school)
        school_cov = -1*school_mat[0][1]
        hospital_mat = analysis.coe(prop,hospital)
        hospital_cov = hospital_mat[0][1]
        print("police_cov:",police_cov)
        print("school_cov:",school_cov)
        print("hospital_cov",hospital_cov)


    def coe(pro,feature):
        price = []
        f = []
        keys = {r['zipcode'] for r in pro}
        for r in pro:
            for i in feature:
                if i['zipcode']==r['zipcode']:
                    price.append(r['avg_value'])
                    f.append(i['count'])
        mat = np.corrcoef(price, f)
        
        return mat
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ruipang_zhou482', 'ruipang_zhou482')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ruipang_zhou482#analysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:totalSchool', {'prov:label':'totalSchool', prov.model.PROV_TYPE:'ont:DataResource'})
        resource2 = doc.entity('dat:police', {'prov:label':'police', prov.model.PROV_TYPE:'ont:DataResource'})
        resource3 = doc.entity('dat:hospital', {'prov:label':'hospital', prov.model.PROV_TYPE:'ont:DataResource'})
        resource4 = doc.entity('dat:propertyAssessment', {'prov:label':'propertyAssessment', prov.model.PROV_TYPE:'ont:DataResource'})
        analysis = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_analysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_analysis,this_script)
        doc.wasGeneratedBy(analysis, get_analysis,endTime)
        doc.wasDerivedFrom(analysis,resource4)
        repo.logout()
        return doc
                           




    








