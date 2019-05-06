import json
import dml
import prov.model
import datetime
import uuid


class OptimizationIdealStations(dml.Algorithm):
    contributor = 'yufeng72'
    reads = ['yufeng72.stationNearbySchool', 'yufeng72.tripToSchool']
    writes = ['yufeng72.idealStations']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yufeng72', 'yufeng72')

        stationsNearbySchool = repo['yufeng72.stationNearbySchool'].find()
        tripsToSchool = repo['yufeng72.tripToSchool'].find()

        collegeStations = []
        for i in stationsNearbySchool:
            collegeStations.append(i)

        collegeTrips = []
        for i in tripsToSchool:
            collegeTrips.append(i)

        # constraint: after add X stations, for every college, tripNum/stationNum < 600, get min(X)
        idealResult = []

        for i in range(len(collegeStations)):
            # print(i)
            stationNum = collegeStations[i]['HubwayStationNearby']
            tripNum = collegeTrips[i]['TripToSchool']
            if stationNum != 0:
                rate = tripNum / stationNum
                # print(i, collegeStations[i]['Name'], rate)
                if rate > 600:
                    newRow = {'Name': collegeStations[i]['Name'], 'NumStudent': collegeStations[i]['NumStudent'],
                              'Latitude': collegeStations[i]['Latitude'], 'Longitude': collegeStations[i]['Longitude'],
                              'HubwayStationNearby': int(tripNum / 900) + 1}
                    idealResult.append(newRow)
                else:
                    idealResult.append(collegeStations[i])
            else:
                # print(i, collegeStations[i]['Name'], 0)
                studentNum = int(collegeStations[i]['NumStudent'])
                newRow = {'Name': collegeStations[i]['Name'], 'NumStudent': collegeStations[i]['NumStudent'],
                          'Latitude': collegeStations[i]['Latitude'], 'Longitude': collegeStations[i]['Longitude'],
                          'HubwayStationNearby': int(studentNum / 3000) + 1}
                idealResult.append(newRow)

        collegeIdealStations = list(idealResult)
        stationAddAll = 0
        for i in range(len(collegeStations)):
            stationNum = collegeStations[i]['HubwayStationNearby']
            idealStationNum = collegeIdealStations[i]['HubwayStationNearby']
            stationAdd = idealStationNum - stationNum
            stationAddAll = stationAddAll + stationAdd
        print("Constraint 1: for colleges with bikestation nearby, tripNum/StationNum should < 900")
        print("Constraint 2: for colleges without bikestation nearby, studentNum/StationNum should < 3000")
        print("To satisfied these constraints, we need to add at least", stationAddAll, "new bike stations.")
        print()

        repo.dropCollection('idealStations')
        repo.createCollection('idealStations')
        repo['yufeng72.idealStations'].insert_many(idealResult)
        repo['yufeng72.idealStations'].metadata({'complete': True})
        print(repo['yufeng72.idealStations'].metadata())

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

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yufeng72', 'yufeng72')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet',
        # 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:yufeng72#GetIdealStations',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        collegeStations = doc.entity('dat:yufeng72#stationNearbySchool',
                                     {prov.model.PROV_LABEL: 'Stations Nearby Colleges',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})

        collegeTrips = doc.entity('dat:yufeng72#tripToSchool',
                                  {prov.model.PROV_LABEL: 'Trips End at Colleges', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_idealStations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_idealStations, this_script)
        doc.used(get_idealStations, collegeStations, startTime)
        doc.used(get_idealStations, collegeTrips, startTime)

        idealStations = doc.entity('dat:yufeng72#idealStations',
                                   {prov.model.PROV_LABEL: 'Ideal Number of Stations for Colleges',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(idealStations, this_script)
        doc.wasGeneratedBy(idealStations, get_idealStations, endTime)
        doc.wasDerivedFrom(collegeStations, idealStations, get_idealStations, get_idealStations, get_idealStations)
        doc.wasDerivedFrom(collegeTrips, idealStations, get_idealStations, get_idealStations, get_idealStations)

        repo.logout()

        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
OptimizationIdealStations.execute()
doc = OptimizationIdealStations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
