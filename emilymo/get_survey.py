import dml
from pymongo import MongoClient
import prov
import datetime
import uuid
import urllib.request
import dbfread
import copy
import os
import shutil

class get_survey(dml.Algorithm):
    contributor = 'emilymo'
    reads = []
    writes = ['a.surv', 'a.survtemp']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
    
        if not os.path.exists('temp'):
            os.makedirs('temp')
        z = urllib.request.urlretrieve('http://datamechanics.io/data/2010_BNS_by_NBHDS38_Shape.dbf', 'temp/2010_BNS_by_NBHDS38_Shape.dbf')
        survraw = dbfread.DBF('temp/2010_BNS_by_NBHDS38_Shape.dbf')
        surv = []
        for e in survraw:
            surv.append(dict(e))
        a['surv'].insert_many(surv)
        
        # consolidate survey neighborhood names and city of boston neighborhood names
        survtemp = copy.deepcopy(surv)
        for e in survtemp:
            # west end / north end --> north end
            if e['NBHDS38_'] == 'West End/North End/Govt Ctr': 
                e['NBHDS38_'] = 'West End/North End'
            # charleston/navy yard --> charleston
            if e['NBHDS38_'] == 'Charlestown/Navy Yard':
                e['NBHDS38_'] = 'Charlestown'
            # dorchester heights/waterfront, south bay/newmarket, blue hill ave, grove hall, south dorchester, columbia point/savin hill, uphams corner, jones hill, fields corner, bowdoin/geneva, codman/ashmont, franklin field, egleston, popes hill, lower mills, forest hills --> dorchester
            if e['NBHDS38_'] == 'Dorchester Heights/Waterfront' or e['NBHDS38_'] == 'South Bay/Newmarket Square' or e['NBHDS38_'] == 'Egleston' or e['NBHDS38_'] == 'Blue Hill Ave' or e['NBHDS38_'] == 'Grove Hall' or e['NBHDS38_'] == 'South Dorchester/Four Corners' or e['NBHDS38_'] == 'Columbia Point/Savin Hill' or e['NBHDS38_'] == 'Uphams Corner' or e['NBHDS38_'] == 'Jones Hill' or e['NBHDS38_'] == 'Fields Corner' or e['NBHDS38_'] == 'Bowdoin/Geneva' or e['NBHDS38_'] == 'Codman/Ashmont' or e['NBHDS38_'] == 'Franklin Field/Norfolk' or e['NBHDS38_'] == 'Popes Hill/Neponset' or e['NBHDS38_'] == 'Lower Mills/Adams' or e['NBHDS38_'] == 'Amer Legion/Mt Hope/Forest Hills':
                e['NBHDS38_'] = 'Dorchester'
            # south end/lower roxbury, orchard park, --> south end
            if e['NBHDS38_'] == 'South End/Lower Roxbury' or e['NBHDS38_'] == 'Orchard Park':
                e['NBHDS38_'] = 'South End'
            #  madison park/dudley, fort hill, roxbury --> roxbury
            if e['NBHDS38_'] == 'Madison Park/Dudley' or e['NBHDS38_'] == 'Fort Hill/Washington Park':
                e['NBHDS38_'] = 'Roxbury'   
            # bromley/heath --> jamaica plain
            if e['NBHDS38_'] == 'Bromley/Heath':
                e['NBHDS38_'] = 'Jamaica Plain'
            # west / east / north mattapan -> mattapan
            if e['NBHDS38_'] == 'West Mattapan' or e['NBHDS38_'] == 'East Mattapan' or e['NBHDS38_'] == 'North Mattapan':
                e['NBHDS38_'] = 'Mattapan'
            # roslindale village --> roslindale
            if e['NBHDS38_'] == 'Roslindale Village':
                e['NBHDS38_'] = 'Roslindale'
            # hyde park s/w / n/e / other --> hyde park
            if e['NBHDS38_'] == 'Hyde Park S/W' or e['NBHDS38_'] == 'Hyde Park N/E' or e['NBHDS38_'] == 'Hyde Park Other':
                e['NBHDS38_'] = 'Hyde Park'
            # change chinatown to downtown/chinatown b/c consolidated in nbhd
            if e['NBHDS38_'] == 'Chinatown':
                e['NBHDS38_'] = 'Downtown/Chinatown'
        a['survtemp'].insert_many(survtemp)
        
        # delete temp folder
        shutil.rmtree('temp')
        
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
    
    
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        doc.add_namespace('dm', 'http://datamechanics.io/data/') 
        
        this_script = doc.agent('alg:emilymo#get_survey', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        get_survey = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        survey_site = doc.entity('dm:2010_BNS_by_NBHDS38_Shape.dbf', {prov.model.PROV_LABEL:'Boston Neighborhoods Site', prov.model.PROV_TYPE:'ont:DataResource'}) 
        surv = doc.entity('dat:emilymo#surv', {prov.model.PROV_LABEL:'Raw Boston Neighborhood Survey Data', prov.model.PROV_TYPE:'ont:DataSet'})
        survtemp = doc.entity('dat:emilymo#survtemp', {prov.model.PROV_LABEL:'Survey Data Sorted by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAssociatedWith(get_survey, this_script)
        doc.wasAttributedTo(surv, this_script)
        doc.wasAttributedTo(survtemp, this_script)
        doc.wasGeneratedBy(surv, get_survey)
        doc.wasGeneratedBy(survtemp, get_survey)
        doc.used(get_survey, survey_site, other_attributes={prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.used(get_survey, surv, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasDerivedFrom(surv, survey_site)
        doc.wasDerivedFrom(survtemp, surv)
        
        return doc

