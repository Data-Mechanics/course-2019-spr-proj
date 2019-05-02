import dml
from pymongo import MongoClient
import prov
import datetime
import uuid
import urllib.request
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

class get_ymcas(dml.Algorithm):
    contributor = 'emilymo'
    reads = []
    writes = ['a.ymca1', 'a.ymcas']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
        
        url = 'https://ymcaboston.org/branches'
        geturl = urllib.request.urlopen(url).read().decode("utf-8")
        sp = bs(geturl, "lxml")
        prelocs = sp.find_all('div', class_= 'supplemental-links')
        # locs = prelocs[0].find_all('span', class_ = 'subsubnavigation__copy') # the website changed
        locs = prelocs[0].find_all('a')
        nms = [nm.text for nm in locs]
        ymca = pd.DataFrame(nms, columns = ['Location'])
        if (trial == True):
            ymca1 = ymca.to_dict('records')[0:5]
        else:
            ymca1 = ymca.to_dict('records')[:-3]
        a['ymca1'].insert_many(ymca1)
        
        # geocode ymcas
        api = dml.auth['services']['googlemaps']['key'] 
        
        import time
        ymcas = []
        failed = []
        base = 'https://maps.googleapis.com/maps/api/geocode/json?' 
        for n in a['ymca1'].find():
            address = n['Location']
            try:
                # print(address)
                url = base + "address=" + "+massachusetts" + address.replace(" ","+") + "&key=" + api
                r = requests.get(url)
                results = r.json()['results']
                location = results[0]['geometry']['location']
                # time.sleep(1)
                geo = location['lat'], location['lng']
            except:
                print('Failed:')
                print(failed.append(address))
                geo = "NA"
            ymcas.append({'ymca':address, 'location':geo})
        a['ymcas'].insert_many(ymcas)
        
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime} 
    
    
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        doc.add_namespace('y', 'https://ymcaboston.org/') # NOT DONE
        doc.add_namespace('gmaps', 'https://maps.googleapis.com/maps/api/geocode/')
        
        
        this_script = doc.agent('alg:emilymo#get_ymcas', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']}) 
        get_ymcas = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        ymca_site = doc.entity('y:branches', {prov.model.PROV_LABEL:'Boston YMCAs Site', prov.model.PROV_TYPE:'ont:DataResource'})
        ymca1 = doc.entity('dat:emilymo#ymca1', {prov.model.PROV_LABEL:'YMCA Names', prov.model.PROV_TYPE:'ont:DataSet'})
        ymcas = doc.entity('dat:emilymo#ymcas', {prov.model.PROV_LABEL:'Geocoded YMCAs', prov.model.PROV_TYPE:'ont:DataSet'})
        gmaps = doc.entity('gmaps:', {prov.model.PROV_LABEL:'Google Maps Geocode', prov.model.PROV_TYPE:'ont:DataResource'})
                          
        doc.wasAssociatedWith(get_ymcas, this_script)
        doc.wasAttributedTo(ymca1, this_script)
        doc.wasAttributedTo(ymcas, this_script)
        doc.wasGeneratedBy(ymca1, get_ymcas)
        doc.wasGeneratedBy(ymcas, get_ymcas)
        doc.used(get_ymcas, ymca_site, other_attributes={prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.used(get_ymcas, ymca1, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.used(get_ymcas, gmaps, other_attributes={prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasDerivedFrom(ymca1, ymca_site)
        doc.wasDerivedFrom(ymcas, ymca1)
        doc.wasDerivedFrom(ymcas, gmaps)
        
        return doc
        
                          
    