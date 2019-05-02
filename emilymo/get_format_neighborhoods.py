import dml
from pymongo import MongoClient
import prov
import datetime
import uuid
import urllib.request
import json
import copy
import os

class get_format_neighborhoods(dml.Algorithm):
    contributor = 'emilymo'
    reads = []
    writes = ['a.nhbd1', 'a.nbhd', 'a.nbhdtemp']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
    
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        geturl = urllib.request.urlopen(url).read().decode("utf-8")
        nbhd1 = json.loads(geturl)['features']
        a['nbhd1'].insert_many(nbhd1)
    
        # reformat 
        for e in a['nbhd1'].find():
            nw = copy.deepcopy(e['properties'])
            nw['coordinates'] = copy.deepcopy(e['geometry']['coordinates'])
            a['nbhd'].insert_one(nw)
            
        # consolidate neighborhood names to match survey
        nbhdtemp = [r for r in a['nbhd'].find()]
        for e in nbhdtemp:
            # allston / brighton are separate in raw nbhd but joined in surv--might need to join them name-wise
            if e['Name'] == 'Allston' or e['Name'] == 'Brighton':
                e['Name'] = 'Allston/Brighton'
            # join longwood / back bay / bay village / beacon hill / fenway from raw nbhd into one category (what is KE??? kenmore?)
            if e['Name'] == 'Longwood' or e['Name'] == 'Back Bay' or e['Name'] == 'Bay Village' or e['Name'] == 'Beacon Hill' or e['Name'] == 'Fenway':
                e['Name'] = 'KE/LONG/BERK/BB/BH'
            # join downtown / chinatown / leather district from raw nbhd into chinatown
            if e['Name'] == 'Downtown' or e['Name'] == 'Chinatown' or e['Name'] == 'Leather District':
                e['Name'] = 'Downtown/Chinatown'
            # join south boston / south boston waterfront into one category
            if e['Name'] == 'South Boston Waterfront':
                e['Name'] = 'South Boston'
            # join west end and north end
            if e['Name'] == 'West End' or e['Name'] == 'North End':
                e['Name'] = 'West End/North End'
        from pymongo.errors import BulkWriteError # new for trycatch:
        from pprint import pprint
        try:
            a['nbhdtemp'].insert_many(nbhdtemp)
        except BulkWriteError as bwe:
            # pprint(bwe.details)
            print(len([r for r in a['nbhdtemp'].find()]))
            
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
    
        


    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        doc.add_namespace('bosop', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        
        this_script = doc.agent('alg:emilymo#get_format_neighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        get_format_neighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        nbhd_site = doc.entity('bosop:3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson', {prov.model.PROV_LABEL:'Boston Neighborhoods Site', prov.model.PROV_TYPE:'ont:DataResource'})
        nbhd1 = doc.entity('dat:emilymo#nbhd1', {prov.model.PROV_LABEL:'Boston Neighborhoods (Old Format)', prov.model.PROV_TYPE:'ont:DataSet'})
        nbhd = doc.entity('dat:emilymo#nbhd', {prov.model.PROV_LABEL:'Boston Neighborhoods (City of Boston Format)', prov.model.PROV_TYPE:'ont:DataSet'})
        nbhdtemp = doc.entity('dat:emilymo#nbhdtemp', {prov.model.PROV_LABEL:'Boston Neighborhoods Regrouped', prov.model.PROV_TYPE:'ont:DataSet'})
                         
        doc.wasAttributedTo(nbhd1, this_script)
        doc.wasAttributedTo(nbhd, this_script)
        doc.wasAttributedTo(nbhdtemp, this_script)
        doc.wasGeneratedBy(nbhd1, get_format_neighborhoods)
        doc.wasGeneratedBy(nbhd, get_format_neighborhoods)
        doc.wasGeneratedBy(nbhdtemp, get_format_neighborhoods)
        doc.wasAssociatedWith(get_format_neighborhoods, this_script)
        doc.used(get_format_neighborhoods, nbhd_site, other_attributes={prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.used(get_format_neighborhoods, nbhd1, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.used(get_format_neighborhoods, nbhd, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasDerivedFrom(nbhd, nbhd1)
        doc.wasDerivedFrom(nbhdtemp, nbhd)
        doc.wasDerivedFrom(nbhd1, nbhd_site)
        
        return doc