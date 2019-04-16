import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from pprint import pprint
from collections import defaultdict
from opencage.geocoder import OpenCageGeocode



# Take all the bike collisions and cross reference to the streets 
# with traffic lights to see if an accident has occured there
# OUTPUT: a dataset aggregating the sum of all bike accidents that have happened at a particular traffic light
class streetlights_collisions(dml.Algorithm):
    contributor = 'nhuang54_tkixi_wud'
    reads = ['nhuang54_tkixi_wud.boston_streetlights','nhuang54_tkixi_wud.boston_collisions']
    writes = ['nhuang54_tkixi_wud.trafficlight_collisions']


    @staticmethod
    def execute(trial = False):

        def select(R, s):
            return [t for t in R if s(t)]
        def project(R, p):
            return [p(t) for t in R]
            

        print("in trafficlights collision transformation")
        
        # { bike collisions : mode_type = bike
          #   "dispatch_ts": "2015-01-01 00:24:27",
          #   "mode_type": "mv",
          #   "location_type": "Intersection",
          #   "street": "",
          #   "xstreet1": "TRAIN ST",
          #   "xstreet2": "WESTGLOW ST",
          #   "x_cord": 777243.68,
          #   "y_cord": 2930930.11,
          #   "lat": 42.2897498978,
          #   "long": -71.0525153263
          # },
        
        
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nhuang54_tkixi_wud', 'nhuang54_tkixi_wud')

        bc = repo.nhuang54_tkixi_wud.boston_collisions
        bt = repo.nhuang54_tkixi_wud.boston_streetlights


        # Boston Collisions 
        # mode_type, xstreet1, xstreet2
        bostonCollisions = bc.find()
        print("###PRINTED Bike Collisions###")

        # select to get all bike collisions
        bikeCollisions = select(bostonCollisions, lambda x: x['mode_type'] == 'bike')

        collision_filter = lambda x: {'xstreet1': x['xstreet1'],
                                     'xstreet2': x['xstreet2'],
                                     'location_type': x['location_type']
                                     }
        collision_project = project(bikeCollisions, collision_filter)
        # {'xstreet1': 'OLNEY ST', 
        #  'xstreet2': 'INWOOD ST', 
        #  'location_type': 'Intersection'}



        # # Boston Traffic Lights
       
        # { traffic lights
        #     "X": -71.07510206235595,
        #     "Y": 42.308188275308936,
        #     "OBJECTID": 1,
        #     "Count_": 386,
        #     "Int_Number": 3121,
        #     "Location": "Columbia Rd. & Wyola Place",
        #     "Dist": "DO"
        # },
        # {"the_geom" : "POINT (-71.07921632936232 42.35482231438127)", 
        #   "OBJECTID" : 10, 
        #   "TYPE" : "LIGHT", 
        #   "Lat" : 42.3548223144, 
        #   "Long" : -71.0792163294
        #   }
           
        api_key = dml.auth['services']['openCagePortal']['api_key']
        geocoder = OpenCageGeocode(api_key)
        api_limit = 0


        data = []
        streetLights = bt.find()
        for x in streetLights:
            lat = x['Lat']
            lng = x['Long']
            print('lat', lat)
            api_limit+=1
            results = geocoder.reverse_geocode(lat, lng)
            print('printing results')
            if 'road' in results[0]['components']:            
                road = results[0]['components']['road']
                print('road', road)
                if api_limit > 15:
                    break
            for y in collision_project:
                break

                # intersection = x['Location']



                # intersection = intersection.replace('Mt.', 'Mount')
                # intersection = intersection.replace('.','')
                # intersection = intersection.upper()
                # # found an intersection that had a bike collision
                # if str(y['xstreet1']) and str(y['xstreet2']) in intersection:
                #     trafficlight_collisions = {}
                #     trafficlight_collisions['traffic_light'] = 1
                #     trafficlight_collisions.update({'intersection': intersection, 'location_type': y['location_type']})
                #     data.append(trafficlight_collisions)
        c = defaultdict(int)
        # how many accidents have happened at each intersection
        for d in data:
            c[d['intersection']] += d['traffic_light'] 

        trafficlight_collision_data = [{'intersection': intersection, 'bike_collisions': traffic_light} for intersection, traffic_light in c.items()]
     
                

        repo.dropCollection("nhuang54_tkixi_wud.trafficlight_collisions")
        repo.createCollection("nhuang54_tkixi_wud.trafficlight_collisions")

        repo['nhuang54_tkixi_wud.trafficlight_collisions'].insert_many(trafficlight_collision_data)
        print("Done with Transformation of Traffic Lights + Bike Collisions")

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}



    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nhuang54_tkixi_wud', 'nhuang54_tkixi_wud')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/?prefix=tkixi/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/')
        
        
        this_script = doc.agent('alg:nhuang54_tkixi_wud#transformation2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:nhuang54_tkixi_wud#crashRecords', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        resource2 = doc.entity('dat:nhuang54_tkixi_wud#trafficSignals', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        transformation2 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transformation2, this_script)

        doc.usage(transformation2, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )
        doc.usage(transformation2, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )

        trafficlightCollision = doc.entity('dat:nhuang54_tkixi_wud#trafficlightCollision', {prov.model.PROV_LABEL:'Number of accidents that have happened at an intersection with traffic lights', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficlightCollision, this_script)
        doc.wasGeneratedBy(trafficlightCollision, transformation2, endTime)
        doc.wasDerivedFrom(trafficlightCollision, resource1, transformation2, transformation2, transformation2)
        doc.wasDerivedFrom(trafficlightCollision, resource2, transformation2, transformation2, transformation2)
        
        


        repo.logout()
        
        return doc

streetlights_collisions.execute()
# doc = transformation2.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof