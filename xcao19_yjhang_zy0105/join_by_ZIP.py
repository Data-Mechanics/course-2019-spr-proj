#!/usr/bin/env python
# coding: utf-8

import dml
import prov.model
import datetime
import uuid
import math

class join_by_ZIP(dml.Algorithm):
    contributor = 'xcao19_yjhang_zy0105'
    reads = ['xcao19_yjhang_zy0105.center', 'xcao19_yjhang_zy0105.centerPool', 'xcao19_yjhang_zy0105.policeStation','xcao19_yjhang_zy0105.school','xcao19_yjhang_zy0105.property']
    writes = ['xcao19_yjhang_zy0105.ZIPCounter']


    def dealWithZip(data,key):
        for i in data:
            if(len(str(int(i[key][:5]))) == 4):
                i[key] = '0'+str(int(i[key][:5]))
    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xcao19_yjhang_zy0105', 'xcao19_yjhang_zy0105')

        def dealWithZip(data, key):
            for i in data:
                if (len(str(int(i[key][:5]))) == 4):
                    i[key] = '0' + str(int(i[key][:5]))

        def count(R,key):
            c = 0
            for r in R:
                if(r['ZIP']==key):
                    c += 1
            return c


        r_center= repo['xcao19_yjhang_zy0105.center']
        r_centerPool=repo['xcao19_yjhang_zy0105.centerPool']
        r_policeStation = repo['xcao19_yjhang_zy0105.policeStation']
        r_property = repo['xcao19_yjhang_zy0105.property']
        r_school = repo['xcao19_yjhang_zy0105.school']

        center=list(r_center.find({},{'_id':0,'ZIP':1,}))
        centerPool=list(r_centerPool.find({},{'_id':0,'ZIP':1}))
        policeStation = list(r_policeStation.find({},{'_id':0,'ZIP':1}))
        property = list(r_property.find({},{'_id':0,'ZIPCODE':1,'AV_TOTAL':1,'GROSS_AREA':1}))
        school = list(r_school.find({},{'_id':0,'ZIP':1}))
        dealWithZip(center,"ZIP")
        dealWithZip(centerPool,"ZIP")
        dealWithZip(policeStation,"ZIP")
        #dealWithProZip(property,"ZIPCODE")
        dealWithZip(school,"ZIP")
        for s in property:
            s["ZIPCODE"] = "0"+ str(s["ZIPCODE"])[:4]
        # print(school)

        val_dic = {}
        ar_dic = {}
        ppty_avg = {}
        #print(property[0])
        for ppty in property:
            #val_dic.setdefault(ppty['ZIPCODE'],0)
            if(ppty['GROSS_AREA'] is not None):
                if(val_dic.__contains__(ppty['ZIPCODE'])):
                    val_dic[ppty['ZIPCODE']]+=ppty['AV_TOTAL']
                    #print(ppty['GROSS_AREA'])
                    ar_dic[ppty['ZIPCODE']]+=ppty['GROSS_AREA']
                else:
                    val_dic[ppty['ZIPCODE']]=ppty['AV_TOTAL']
                    ar_dic[ppty['ZIPCODE']]=ppty['GROSS_AREA']
        #print(val_dic)

        for k in val_dic:
            if(ar_dic[k]!=0):
                ppty_avg[k] = val_dic[k]/ar_dic[k]
        #print(ppty_avg)

        res = []
        for key in ppty_avg:
            res.append({'ZIP':key,'val_avg':ppty_avg[key]})
        #print(res)
        for r in res:
            r['centerNum']=count(center,r['ZIP'])
            r['centerPoolNum'] = count(centerPool,r['ZIP'])
            r['policeStationNum'] = count(policeStation,r['ZIP'])
            r['schoolNum'] = count(school,r['ZIP'])
        print(res)

        repo.dropCollection("ZIPCounter")
        repo.createCollection("ZIPCounter")
        repo["xcao19_yjhang_zy0105.ZIPCounter"].insert_many(res)
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}             

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''   
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        # New
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        agent = doc.agent('alg:xcao19_yjhang_zy0105#join_by_ZIP',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
     
        property = doc.entity('dat:xcao19_yjhang_zy0105#property',
                           {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        school = doc.entity('dat:xcao19_yjhang_zy0105#school',
                               {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        center = doc.entity('dat:xcao19_yjhang_zy0105#center',
                               {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        centerPool = doc.entity('dat:xcao19_yjhang_zy0105#centerPool',
                               {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        policeStation = doc.entity('dat:xcao19_yjhang_zy0105#policeStation',
                               {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        count_all_by_zip = doc.entity('dat:xcao19_yjhang_zy0105#count_all_by_zip',
                               {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        activity = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(activity, agent)

        doc.usage(activity, property, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:product select and project'}
                  )
        doc.usage(activity, school, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:product select and project'}
                  )
        doc.usage(activity, centerPool, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:select and aggregate'}
                  )
        doc.usage(activity, center, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:select and aggregate'}
                  )
        doc.usage(activity, policeStation, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:select and aggregate'}
                  )
        doc.wasDerivedFrom(count_all_by_zip, school, activity, activity, activity)
        doc.wasDerivedFrom(count_all_by_zip, centerPool, activity, activity, activity)
        doc.wasDerivedFrom(count_all_by_zip, center, activity, activity, activity)
        doc.wasDerivedFrom(count_all_by_zip, policeStation, activity, activity, activity)
        doc.wasDerivedFrom(count_all_by_zip, property, activity, activity, activity)

        doc.wasAttributedTo(count_all_by_zip, agent)
        doc.wasGeneratedBy(count_all_by_zip, activity, endTime)

        return doc
