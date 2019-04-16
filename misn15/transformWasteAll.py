import urllib.request
import dml
import json
import prov.model
import datetime
import uuid
import copy
from pyproj import Proj, transform
from uszipcode import SearchEngine
import requests
from geopy.geocoders import Nominatim

class transformWasteAll(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.hwgen', 'misn15.aul', 'misn15.waste']
    writes = ['misn15.waste_all']

    @staticmethod
    def execute(trial = False):
        '''Transform waste data for city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        hwgen = []
        hwgen = repo['misn15.hwgen'].find()
        hwgen = copy.deepcopy(hwgen)

        aul = []
        aul = repo['misn15.aul'].find()
        aul = copy.deepcopy(aul)

        waste = []
        waste = repo['misn15.waste'].find()
        waste = copy.deepcopy(waste)

        # search for coordinates based on address
        geolocator = Nominatim(user_agent = "mis", timeout = 5)

        # get csr for coordinate search
        inProj = Proj(init='epsg:26986')
        outProj = Proj(init='epsg:4326')

        # project coordinates as US census tract number and zipcode
        search = SearchEngine(simple_zipcode=True)

        if trial:
            # filter hwgen
            hwgen_list = []
            i = 0
            for x in hwgen:
                if i < 5:
                    hwgen_list += [[x['Name'], x['Address'], x['Town'], x['ZIP Code'], x['RCRA Gen Status']]]
                    i += 1
                else:
                    break

            # get coordinates for hwgen
            for x in hwgen_list:
                full_address = str(x[1] + ' ' + x[2] + ' ' + 'MASSACHUSETTS')
                location = geolocator.geocode(full_address)
                if location is not None:
                    x += [[location.longitude, location.latitude]]
                    params = urllib.parse.urlencode({'latitude': location.latitude, 'longitude': location.longitude, 'format': 'json'})
                    url = 'https://geo.fcc.gov/api/census/block/find?' + params
                    response = requests.get(url)
                    data = response.json()
                    geoid = data['Block']['FIPS'][0:11]
                    x += [geoid]

            # get zipcodes in correct format
            for x in hwgen_list:
                if x[3][0] != '0':
                    zipcode_num = '0' + x[3]
                elif len(x[3]) != 5:
                    zipcode_num = x[3][0:5]
                if x[3][0] == 0 and x[3][1] == 0:
                    zipcode_num = x[3][1:6]
                x[3] = zipcode_num

            # filter aul
            aul_list = []
            i = 0
            for x in aul:
                if i < 25:
                    if x['properties']['TOWN'] == 'BOSTON':
                        result = search.by_coordinates(x['geometry']['coordinates'][1], x['geometry']['coordinates'][0], returns=1)
                        params = urllib.parse.urlencode({'latitude': x['geometry']['coordinates'][1], 'longitude': x['geometry']['coordinates'][0], 'format': 'json'})
                        url = 'https://geo.fcc.gov/api/census/block/find?' + params
                        response = requests.get(url)
                        data = response.json()
                        result = result[0]
                        geoid = data['Block']['FIPS'][0:11]
                        aul_list += [[x['properties']['NAME'], x['properties']['ADDRESS'], x['properties']['TOWN'],
                                      result.zipcode, x['properties']['STATUS'], x['geometry']['coordinates'], geoid]]
                        i += 1
                else:
                    break

            # filter waste
            waste_list = []
            i = 0
            for x in waste:
                if i < 25:
                    if x['properties']['TOWN'] == 'BOSTON':
                        long, lat = transform(inProj, outProj, x['geometry']['coordinates'][0], x['geometry']['coordinates'][1])
                        result = search.by_coordinates(lat, long, returns=1)
                        result = result[0]
                        params = urllib.parse.urlencode({'latitude': lat, 'longitude': long, 'format': 'json'})
                        url = 'https://geo.fcc.gov/api/census/block/find?' + params
                        response = requests.get(url)
                        data = response.json()
                        geoid = data['Block']['FIPS'][0:11]
                        waste_list += [[x['properties']['NAME'], x['properties']['ADDRESS'], x['properties']['TOWN'],
                                        result.zipcode, x['properties']['STATUS'], [long, lat], geoid]]
                        i += 1
                else:
                    break

            # merge all waste data
            waste_all = hwgen_list + aul_list + waste_list

        else:
            # filter hwgen
            hwgen_list = []
            for x in hwgen:
                hwgen_list += [[x['Name'], x['Address'], x['Town'], x['ZIP Code'], x['RCRA Gen Status']]]

            # get coordinates for hwgen
            for x in hwgen_list:
                full_address = str(x[1] + ' ' + x[2] + ' ' + 'MASSACHUSETTS')
                location = geolocator.geocode(full_address)
                if location is not None:
                    x += [[location.longitude, location.latitude]]
                    params = urllib.parse.urlencode(
                        {'latitude': location.latitude, 'longitude': location.longitude, 'format': 'json'})
                    url = 'https://geo.fcc.gov/api/census/block/find?' + params
                    response = requests.get(url)
                    data = response.json()
                    geoid = data['Block']['FIPS'][0:11]

            # get zipcodes in correct format
            for x in hwgen_list:
                if x[3][0] != '0':
                    zipcode_num = '0' + x[3]
                elif len(x[3]) != 5:
                    zipcode_num = x[3][0:5]
                if x[3][0] == 0 and x[3][1] == 0:
                    zipcode_num = x[3][1:6]
                x[3] = zipcode_num

            # filter aul
            aul_list = []
            for x in aul:
                if x['properties']['TOWN'] == 'BOSTON':
                    result = search.by_coordinates(x['geometry']['coordinates'][1], x['geometry']['coordinates'][0], returns=1)
                    params = urllib.parse.urlencode({'latitude': x['geometry']['coordinates'][1],
                                                     'longitude': x['geometry']['coordinates'][0], 'format': 'json'})
                    url = 'https://geo.fcc.gov/api/census/block/find?' + params
                    response = requests.get(url)
                    data = response.json()
                    result = result[0]
                    geoid = data['Block']['FIPS'][0:11]
                    aul_list += [[x['properties']['NAME'], x['properties']['ADDRESS'], x['properties']['TOWN'],
                                  result.zipcode, x['properties']['STATUS'], x['geometry']['coordinates'], geoid]]

            # filter waste
            waste_list = []
            for x in waste:
                if x['properties']['TOWN'] == 'BOSTON':
                    long, lat = transform(inProj, outProj, x['geometry']['coordinates'][0], x['geometry']['coordinates'][1])
                    result = search.by_coordinates(lat, long, returns=1)
                    result = result[0]
                    params = urllib.parse.urlencode({'latitude': lat, 'longitude': long, 'format': 'json'})
                    url = 'https://geo.fcc.gov/api/census/block/find?' + params
                    response = requests.get(url)
                    data = response.json()
                    geoid = data['Block']['FIPS'][0:11]
                    waste_list += [[x['properties']['NAME'], x['properties']['ADDRESS'], x['properties']['TOWN'],
                                    result.zipcode, x['properties']['STATUS'], [round(long, 2), round(lat, 2)], geoid]]

            # merge all waste data
            waste_all = hwgen_list + aul_list + waste_list

        repo.dropCollection("misn15.waste_all")
        repo.createCollection("misn15.waste_all")

        for x in waste_all:
            entry = {'Name': x[0], 'Address': x[1], 'Zip Code': x[3], 'Coordinates': x[5], 'Status': x[4], 'FIPS': x[6]}
            repo['misn15.waste_all'].insert_one(entry)

        repo['misn15.waste_all'].metadata({'complete':True})
        print(repo['misn15.waste_all'].metadata())

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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        this_script = doc.agent('alg:transformWasteAll', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:hwgen', {'prov:label':'Boston Hazardous Waste/Oil Sites', prov.model.PROV_TYPE:'ont:DataResource'})
        resource2 = doc.entity('dat:aul', {'prov:label':'Boston Hazardous Waste with Limited Use', prov.model.PROV_TYPE:'ont:DataResource'})
        resource3 = doc.entity('dat:waste', {'prov:label': 'Boston Hazardous Waste', prov.model.PROV_TYPE: 'ont:DataResource'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                        }
                  )
        doc.usage(this_run, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                        }
                  )
        doc.usage(this_run, resource3, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )
        resource4 = doc.entity('dat:waste_all', {prov.model.PROV_LABEL:'Master List of Waste Sites', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(resource4, this_script)
        doc.wasGeneratedBy(resource4, this_run, endTime)
        doc.wasDerivedFrom(resource4, resource, this_run, this_run, this_run)

        return doc

transformWasteAll.execute()
doc = transformWasteAll.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
