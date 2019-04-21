import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import json

import yelpfusion as yf
auth = json.load(open('../auth.json', 'r'))
API_KEY= auth['services']['yelpfusionportal']['key']

class stations(dml.Algorithm):
    contributor = 'ctrinh_fat60221_veeyn'
    reads = []
    writes = ['ctrinh_fat60221_veeyn.stations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')

        resdict = {}

        route = "https://api-v3.mbta.com/stops?filter[route]=Green-B"
        response = urllib.request.urlopen(route).read().decode("utf-8")
        r = json.loads(response)
        reslist = []

        if trial == True:
            switchTrial = 2
        else:
            switchTrial = len(r['data'])

        # print(len(r['data']))

        for i in range(switchTrial):
            rindict = {}
            name = r['data'][i]['attributes']['name']
            sid = r['data'][i]['id']
            latitude = r['data'][i]['attributes']['latitude']
            longitude = r['data'][i]['attributes']['longitude']
            address = r['data'][i]['attributes']['address']
            rindict['name'] = name
            rindict['stop_id'] = sid
            rindict['latitude'] = latitude
            rindict['longitude'] = longitude
            rindict['address'] = address
            if name == "South Street":
                rindict["address"] = "43 South Street, Boston, MA"
                address = "43 South Street, Boston, MA"
            yfres = yf.search(API_KEY, "coffee shop", address)
            csres = []
            for j in range(len(yfres['businesses'])):
                # print(j)
                csdict = {}
                csdict["name"] = yfres['businesses'][j]['name'].replace(".", "")
                csdict["coordinates"] = yfres['businesses'][j]['coordinates']
                csdict["address"] = yfres['businesses'][j]['location']['display_address']
                csres.append(csdict)
            rindict['coffee_shops'] = csres
            reslist.append(rindict)

        resdict['Green Line B'] = reslist

        route = "https://api-v3.mbta.com/stops?filter[route]=Green-C"
        response = urllib.request.urlopen(route).read().decode("utf-8")
        r = json.loads(response)
        reslist = []

        if trial == True:
            switchTrial = 2
        else:
            switchTrial = len(r['data'])

        for i in range(switchTrial):
            rindict = {}
            name = r['data'][i]['attributes']['name']
            sid = r['data'][i]['id']
            latitude = r['data'][i]['attributes']['latitude']
            longitude = r['data'][i]['attributes']['longitude']
            address = r['data'][i]['attributes']['address']
            rindict['name'] = name
            rindict['stop_id'] = sid
            rindict['latitude'] = latitude
            rindict['longitude'] = longitude
            rindict['address'] = address
            yfres = yf.search(API_KEY, "coffee shop", address)
            csres = []
            for j in range(len(yfres['businesses'])):
                csdict = {}
                csdict["name"] = yfres['businesses'][j]['name'].replace(".", "")
                csdict["coordinates"] = yfres['businesses'][j]['coordinates']
                csdict["address"] = yfres['businesses'][j]['location']['display_address']
                csres.append(csdict)
            rindict['coffee_shops'] = csres
            reslist.append(rindict)

        resdict['Green Line C'] = reslist

        route = "https://api-v3.mbta.com/stops?filter[route]=Green-D"
        response = urllib.request.urlopen(route).read().decode("utf-8")
        r = json.loads(response)
        reslist = []

        if trial == True:
            switchTrial = 2
        else:
            switchTrial = len(r['data'])

        for i in range(switchTrial):
            rindict = {}
            name = r['data'][i]['attributes']['name']
            sid = r['data'][i]['id']
            latitude = r['data'][i]['attributes']['latitude']
            longitude = r['data'][i]['attributes']['longitude']
            address = r['data'][i]['attributes']['address']
            rindict['name'] = name
            rindict['stop_id'] = sid
            rindict['latitude'] = latitude
            rindict['longitude'] = longitude
            rindict['address'] = address
            yfres = yf.search(API_KEY, "coffee shop", address)
            csres = []
            for j in range(len(yfres['businesses'])):
                csdict = {}
                csdict["name"] = yfres['businesses'][j]['name'].replace(".", "")
                csdict["coordinates"] = yfres['businesses'][j]['coordinates']
                csdict["address"] = yfres['businesses'][j]['location']['display_address']
                csres.append(csdict)
            rindict['coffee_shops'] = csres
            reslist.append(rindict)

        resdict['Green Line D'] = reslist

        route = "https://api-v3.mbta.com/stops?filter[route]=Green-E"
        response = urllib.request.urlopen(route).read().decode("utf-8")
        r = json.loads(response)
        reslist = []

        if trial == True:
            switchTrial = 2
        else:
            switchTrial = len(r['data'])

        for i in range(switchTrial):
            rindict = {}
            name = r['data'][i]['attributes']['name']
            sid = r['data'][i]['id']
            latitude = r['data'][i]['attributes']['latitude']
            longitude = r['data'][i]['attributes']['longitude']
            address = r['data'][i]['attributes']['address']
            rindict['name'] = name
            rindict['stop_id'] = sid
            rindict['latitude'] = latitude
            rindict['longitude'] = longitude
            rindict['address'] = address
            yfres = yf.search(API_KEY, "coffee shop", address)
            csres = []
            for j in range(len(yfres['businesses'])):
                csdict = {}
                csdict["name"] = yfres['businesses'][j]['name'].replace(".", "")
                csdict["coordinates"] = yfres['businesses'][j]['coordinates']
                csdict["address"] = yfres['businesses'][j]['location']['display_address']
                csres.append(csdict)
            rindict['coffee_shops'] = csres
            reslist.append(rindict)

        resdict['Green Line E'] = reslist

        # resdict
        
        r = [resdict]

        # sout = open("mbta-stations.txt", "w")
        # print(json.dumps(resdict), file=sout)

        repo.dropCollection("stations")
        repo.createCollection("stations")
        repo['ctrinh_fat60221_veeyn.stations'].insert_many(r)
        repo['ctrinh_fat60221_veeyn.stations'].metadata({'complete':True})
        print(repo['ctrinh_fat60221_veeyn.stations'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        # doc.add_namespace('ylp', 'https://api.yelp.com/')
        doc.add_namespace('mbt', 'https://api-v3.mbta.com/')

        this_script = doc.agent('alg:ctrinh_fat60221_veeyn#stations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbt:stops?filter[route]=?', {'prov:label':'MBTA / Yelp Fusion', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stations, this_script)
        doc.usage(get_stations, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        stations = doc.entity('dat:ctrinh_fat60221_veeyn#stations', {prov.model.PROV_LABEL:'MBTA Station / Coffee Shop Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)
        doc.wasDerivedFrom(stations, resource, get_stations, get_stations, get_stations)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# stations.execute()
# doc = stations.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
