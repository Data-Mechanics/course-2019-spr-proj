import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from pprint import pprint
from collections import defaultdict
from opencage.geocoder import OpenCageGeocode



# Take all the bike collisions and cross reference to the streetlights 
# OUTPUT: a dataset aggregating the total number of bike accidents on a specific street in addition
# to the total number of traffic lights that street has per accident
class streetlights_collisions(dml.Algorithm):
    contributor = 'nhuang54_tkixi_wud'
    reads = ['nhuang54_tkixi_wud.boston_streetlights','nhuang54_tkixi_wud.boston_collisions']
    writes = ['nhuang54_tkixi_wud.trafficlight_collisions']


    @staticmethod
    def execute(trial = False):
        print('finding streetlight collisions')

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



        # # Boston Street Lights
       
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
            if trial:
                if api_limit > 50:
                    break
            if api_limit > 500:
                break
            lat = x['Lat']
            lng = x['Long']
            print('lat', lat)
            api_limit+=1
            results = geocoder.reverse_geocode(lat, lng)
            print('printing results')
            if 'road' in results[0]['components']:            
                road = results[0]['components']['road']
                road = road.replace('Street', 'St')
                road = road.replace('Drive', 'Dr')
                road = road.replace('Avenue', 'Ave')
                road = road.replace('Court', 'Ct')
                road = road.replace('Highway', 'Hwy')
                road = road.replace('Parkway', 'Pkwy')
                road = road.replace('Road', 'Rd')
                road = road.replace('Boulevard', 'Blvd')
                road = road.upper()
            else:
                continue
            
            for y in collision_project:
                xstreet1 = str(y['xstreet1'])
                xstreet2 = str(y['xstreet2'])
                if road in xstreet1 or road in xstreet2:
                    streetlight_collisions = {}
                    streetlight_collisions['streetlight'] = 1
                    streetlight_collisions.update({'road': road})
                    data.append(streetlight_collisions)

        c = defaultdict(int)
        # how many accidents have happened at each intersection
        for d in data:
            c[d['road']] += d['streetlight'] 


        e = defaultdict(int)
        for f in collision_project:
            e[f['xstreet1']] += 1
            e[f['xstreet2']] += 1
        # print(e)

        data2 = []
        streetlight_collision_data = [{'road': road, 'streetlight': streetlight} for road, streetlight in c.items()]
        # print(streetlight_collision_data)
        for x in streetlight_collision_data:
            if x['road'] in e:
                # print(e)
                match = {}
                match.update(x)
                match.update({'collisions': e.get(x['road'])})
                data2.append(match)
        # print(data2)
        
        # data2 = [{'road': 'MARLBOROUGH ST', 'streetlight': 126, 'collisions': 6}, {'road': 'BACK ST', 'streetlight': 6, 'collisions': 3}, {'road': 'BEACON ST', 'streetlight': 1113, 'collisions': 43}, {'road': 'SAINT BOTOLPH ST', 'streetlight': 54, 'collisions': 6}, {'road': 'HUNTINGTON AVE', 'streetlight': 726, 'collisions': 26}, {'road': 'RING RD', 'streetlight': 4, 'collisions': 4}, {'road': 'EXETER ST', 'streetlight': 63, 'collisions': 7}, {'road': 'BOYLSTON ST', 'streetlight': 1326, 'collisions': 34}, {'road': 'NEWBURY ST', 'streetlight': 480, 'collisions': 10}, {'road': 'DARTMOUTH ST', 'streetlight': 546, 'collisions': 13}, {'road': 'COMMONWEALTH AVE', 'streetlight': 2400, 'collisions': 60}, {'road': 'SAINT JAMES AVE', 'streetlight': 64, 'collisions': 4}, {'road': 'STUART ST', 'streetlight': 49, 'collisions': 7}, {'road': 'CLARENDON ST', 'streetlight': 84, 'collisions': 3}]

        repo.dropCollection("nhuang54_tkixi_wud.streetlight_collisions")
        repo.createCollection("nhuang54_tkixi_wud.streetlight_collisions")

        repo['nhuang54_tkixi_wud.streetlight_collisions'].insert_many(data2)
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
        
        
        this_script = doc.agent('alg:nhuang54_tkixi_wud#streetlightcollision_transformation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:nhuang54_tkixi_wud#streetlight_locations', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        resource2 = doc.entity('dat:nhuang54_tkixi_wud#bike_collisions', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        streetlightcollision_transformation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(streetlightcollision_transformation, this_script)

        doc.usage(streetlightcollision_transformation, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )
        doc.usage(streetlightcollision_transformation, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )

        streetlightCollision = doc.entity('dat:nhuang54_tkixi_wud#streetlightCollision', {prov.model.PROV_LABEL:'Number of accidents that have happened at a particular street with the number of streetlights present per accident', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(streetlightCollision, this_script)
        doc.wasGeneratedBy(streetlightCollision, streetlightcollision_transformation, endTime)
        doc.wasDerivedFrom(streetlightCollision, resource1, streetlightcollision_transformation, streetlightcollision_transformation, streetlightcollision_transformation)
        doc.wasDerivedFrom(streetlightCollision, resource2, streetlightcollision_transformation, streetlightcollision_transformation, streetlightcollision_transformation)
        
        


        repo.logout()
        
        return doc

if __name__ == '__main__':
    streetlights_collisions.execute(trial=True)

# streetlights_collisions.execute()
# doc = transformation2.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof