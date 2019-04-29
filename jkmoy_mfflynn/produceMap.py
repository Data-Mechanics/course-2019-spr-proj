import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import sqrt
import folium

def project(R, p):
    return [p(t) for t in R]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def select(R, s):
    return [t for t in R if s(t)]

def not_null(r):
    return (r[0] != None and r[1] != None) and ((r[0]) != '-1' and (r[1] != '-1'))

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

class produceMap(dml.Algorithm):
    contributor = 'jkmoy_mfflynn'
    reads = ['jkmoy_mfflynn.accidentLongLat', 'jkmoy_mfflynn.crimeLongLat']
    writes = []

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')
        
        acc = list(repo['jkmoy_mfflynn.accidentLongLat'].find(projection={'_id':0}))
        crm = list(repo['jkmoy_mfflynn.crimeLongLat'].find(projection={'_id':0}))

        seriousCrimeList = ['Larceny',
                     'Search Warrants',
                     'Drug Violation',
                     'Larceny From Motor Vehicle',
                     'Simple Assault',
                     'Residential Burglary',
                     'Confidence Games',
                     'Offenses Against Child / Family',
                     'Vandalism',
                     'Auto Theft',
                     'Warrant Arrests',
                     'Property Related Damage',
                     'Firearm Violations',
                     'Fraud',
                     'Liquor Violation',
                     'Robbery',
                     'Aggravated Assault',
                     'Counterfeiting',
                     'Prostitution',
                     'Arson',
                     'Commericial Burglary',
                     'Disorderly Conduct',
                     'Ballistics',
                     'Firearm Discovery',
                     'Homicide',
                     'Prisoner Related Incidents',
                     'Operating Under the Influence',
                     'Bomb Hoax',
                     'Criminal Harassment',
                     'Explosives',
                     'HOME INVASION',
                     'Manslaughter',
                     'Biological Threat',
                     'HUMAN TRAFFICKING - INVOLUNTARY SERVITUDE',
                     'HUMAN TRAFFICKING']
            
        # need to find stats per cluster
        arr = [None for _ in range(len(acc))]
        for i in range(len(acc)):
            arr[i] = {'mv':0, 'ped':0, 'bike':0, 'total':0}
            mvCount, pedCount, bikeCount = 0, 0, 0
            for x in acc[i]['types']:
                if x == 'mv': mvCount += 1
                elif x == 'ped': pedCount += 1
                else: bikeCount += 1
            arr[i]['mv'] = mvCount
            arr[i]['ped'] = pedCount
            arr[i]['bike'] = bikeCount
            arr[i]['total'] = mvCount + pedCount + bikeCount
        
        colors = [
                    'red',
                    'blue',
                    'gray',
                    'darkred',
                    '#ECF023',
                    'orange',
                    'beige',
                    'green',
                    'darkgreen',
                    'lightgreen',
                    'darkblue',
                    'lightblue',
                    'purple',
                    '#FFAA50',
                    'pink',
                    'cadetblue',
                    'lightgray',
                    'black',
                    '#3186cc',
                    '#77FF33',
                    '#33FCFF',
                    '#FFFF33',
                    '#FF33D7'
                 ]
        colorCounter = 0
        clusterCounter = 0
        accidentMap = folium.Map(location = [42.328764, -71.096742], tiles = 'Stamen Toner', zoom_start = 12)
        for c in acc:
            clr = colors[colorCounter]
            pu = ('Cluster #' + str(clusterCounter)) + ', Color: ' + clr
            pu += ', Size: ' + str(c['num_points'])
            pu += ', mv: ' + str(arr[clusterCounter]['mv'])
            pu += ', ped: ' + str(arr[clusterCounter]['ped'])
            pu += ', bike: ' + str(arr[clusterCounter]['bike'])
            obj = folium.Popup(pu, max_width=450)
            #markerIcon = folium.Icon(color=colors[colorCounter])
            folium.Marker(location = c['mean'], popup = obj).add_to(accidentMap)
            for x in (c['mean_points']):
                a = str(type(x[0])).split('\'')
                b = str(type(x[1])).split('\'')
                if(a[1] != 'float' or b[1] != 'float'):
                    x[0] = float(x[0])
                    x[1] = float(x[1])
                folium.CircleMarker(location = x, radius = .05, color=clr).add_to(accidentMap)
            colorCounter += 1
            clusterCounter += 1
        # This might have to go into a separate file.
        accidentMap.save('indexAccident.html')

        crimeArr = [0 for _ in range(len(crm))]
        for i in range(len(crm)):
            count = 0
            for j in crm[i]['types']:
                if j in seriousCrimeList:
                    count += 1
            crimeArr[i] = count

        colorCounter = 0
        clusterCounter = 0
        crimeMap = folium.Map(location = [42.328764, -71.096742], tiles = 'Stamen Toner', zoom_start = 12)
        for c in crm:
            clr = colors[colorCounter]
            pu = ('Cluster #' + str(clusterCounter)) + ', Color: ' + clr
            pu += ', Size: ' + str(c['num_points'])
            pu += ', # of Serious Crimes: ' + str(crimeArr[clusterCounter])
            pu += '/(' + str(round(crimeArr[clusterCounter]/c['num_points']*100, 2)) + '%)'
            obj = folium.Popup(pu, max_width=450)
            folium.Marker(location = c['mean'], popup = obj).add_to(crimeMap)
            for x in (c['mean_points'][:1000]):
                a = str(type(x[0])).split('\'')
                b = str(type(x[1])).split('\'')
                if(a[1] != 'float' or b[1] != 'float'):
                    x[0] = float(x[0])
                    x[1] = float(x[1])
                folium.CircleMarker(location = x, radius = .05, color=clr).add_to(crimeMap)
            colorCounter += 1
            clusterCounter += 1
        # This might have to go into a separate file.
        crimeMap.save('indexCrime.html')
        
        '''
        repo.dropCollection('jkmoy_mfflynn.accidentStats')
        repo.createCollection('jkmoy_mfflynn.accidentStats')
        
        repo['jkmoy_mfflynn.accidentStats'].insert_many(final_dataset)
        repo['jkmoy_mfflynn.accidentStats'].metadata({'complete': True})

        print(repo['jkmoy_mfflynn.accidentStats'].metadata())
        '''
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
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jkmoy_mfflynn') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/jkmoy_mfflynn') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:jkmoy_mfflynn#accidentMap', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:accMap', {'prov:label':'Map of accidents', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_a_map = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_a_map, this_script)
        doc.usage(get_a_map, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        a_map = doc.entity('dat:jkmoy_mfflynn#points', {prov.model.PROV_LABEL:'Map points', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(a_map, this_script)
        doc.wasGeneratedBy(a_map, get_a_map, endTime)
        doc.wasDerivedFrom(a_map, resource, get_a_map, get_a_map, get_a_map)

        repo.logout()
                  
        return doc
        

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
averagePerDepartment.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
produceMap.execute()
## eof
