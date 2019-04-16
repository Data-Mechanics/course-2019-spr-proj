import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import z3
import copy
from uszipcode import SearchEngine
from pyproj import Proj, transform

class z3_optimize(dml.Algorithm):
    contributor = 'misn15'
    reads = []
    writes = ['misn15.health']

    @staticmethod
    def execute(trial = False):
        '''Retrieve health data for datamechanics.io'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        waste_all = []
        waste_all = repo['misn15.waste_all'].find()
        waste_all = copy.deepcopy(waste_all)

        schools = []
        schools = repo['misn15.schools'].find()
        schools = copy.deepcopy(schools)

        # get csr for coordinate search
        inProj = Proj(init='epsg:26986')
        outProj = Proj(init='epsg:4326')

        schools_list = []
        for x in schools:
            if x['properties']['TOWN'] == 'BOSTON':
                #schools_list += [[x['geometry'], x['Address'], [0, 0], x['ZIP Code']]]
                long, lat = transform(inProj, outProj, x['geometry']['coordinates'][0], x['geometry']['coordinates'][1])
                schools_list += [[lat, long]]

        waste_list = []
        for x in waste_all:
            waste_list += [[x['Coordinates']]]

        solver = z3.Solver()

        all_coords = [[z3.Int(site) for site in waste_list] for s in schools_list]

        (x1, x2, x3, x4, x5, x6, x7) = [z3.Real('x' + str(i)) for i in range(1, 8)]
        S = z3.Solver()

        # Only allow non-negative flows.
        for x in (x1, x2, x3, x4, x5, x6, x7):
            S.add(x >= 0)

        # Edge capacity constraints.
        S.add(x2 <= 7, x3 <= 8, x4 <= 6)
        S.add(x5 <= 3, x6 <= 4, x7 <= 5)

        # Constraints derived from graph topology.
        S.add(x1 == x2 + x3, x2 == x4 + x5, x3 + x4 == x6, x5 + x6 == x7)

        S.add(x1 > 0)  # We want a positive flow.

        print(S.check())
        print(S.model())

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
        doc.add_namespace('bdp', 'https://chronicdata.cdc.gov/resource/csmm-fdhi.json?cityname=Boston')

        this_script = doc.agent('alg:misn15#getHealth', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:Boston_health', {'prov:label':'Boston_health', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_health = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_health, this_script)
        doc.usage(get_health, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?cityname=Boston'
                  }
                  )

        health = doc.entity('dat:misn15#health', {prov.model.PROV_LABEL:'Boston Health', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(health, this_script)
        doc.wasGeneratedBy(health, get_health, endTime)
        doc.wasDerivedFrom(health, resource, get_health, get_health, get_health)
                  
        return doc

getHealth.execute()
doc = getHealth.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
