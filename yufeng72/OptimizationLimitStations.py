import json
import dml
import prov.model
import datetime
import uuid


class OptimizationLimitStations(dml.Algorithm):
    contributor = 'yufeng72'
    reads = ['yufeng72.stationNearbySchool', 'yufeng72.tripToSchool']
    writes = ['yufeng72.limitStations']

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

        # constraint: after add x (x = 1 in this case) stations, get min average tripNum/stationNum
        limitResult = []

        collegeNumber = len(collegeStations)
        pos = list(range(collegeNumber))
        # print(pos)

        def averageTripStationRateAddOne(position):
            rateSum = 0.0
            for index in range(collegeNumber):
                stationNum = collegeStations[index]['HubwayStationNearby']
                tripNum = collegeTrips[index]['TripToSchool']
                if index == position:
                    stationNum = stationNum + 1
                if stationNum != 0:
                    rate = tripNum / stationNum
                    rateSum = rateSum + rate
            average = rateSum / collegeNumber
            # print(position)
            # print(collegeStations[position]['Latitude'])
            # print(collegeStations[position]['Longitude'])
            # print(average)
            return average

        def metric(position):
            return averageTripStationRateAddOne(position)

        result = min(pos, key=metric)

        for i in range(collegeNumber):
            if i == result:
                newRow = {'Name': collegeStations[i]['Name'], 'NumStudent': collegeStations[i]['NumStudent'],
                          'Latitude': collegeStations[i]['Latitude'], 'Longitude': collegeStations[i]['Longitude'],
                          'HubwayStationNearby': collegeStations[i]['HubwayStationNearby'] + 1}
            else:
                newRow = {'Name': collegeStations[i]['Name'], 'NumStudent': collegeStations[i]['NumStudent'],
                          'Latitude': collegeStations[i]['Latitude'], 'Longitude': collegeStations[i]['Longitude'],
                          'HubwayStationNearby': collegeStations[i]['HubwayStationNearby']}

            limitResult.append(newRow)

        print("Constraint: after add X bike station, get min average tripNum/StationNum for all colleges")
        print("To satisfied the constraint with X = 1, we need to add new bike station at " + collegeStations[result]['Name'])
        print()

        repo.dropCollection('limitStations')
        repo.createCollection('limitStations')
        repo['yufeng72.limitStations'].insert_many(limitResult)
        repo['yufeng72.limitStations'].metadata({'complete': True})
        print(repo['yufeng72.limitStations'].metadata())

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

        this_script = doc.agent('alg:yufeng72#GetLimitStations',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        collegeStations = doc.entity('dat:yufeng72#stationNearbySchool',
                                     {prov.model.PROV_LABEL: 'Stations Nearby Colleges',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})

        collegeTrips = doc.entity('dat:yufeng72#tripToSchool',
                                  {prov.model.PROV_LABEL: 'Trips End at Colleges', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_limitStations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_limitStations, this_script)
        doc.used(get_limitStations, collegeStations, startTime)
        doc.used(get_limitStations, collegeTrips, startTime)

        limitStations = doc.entity('dat:yufeng72#limitStations',
                                   {prov.model.PROV_LABEL: 'Best Placement of X new Stations around Colleges',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(limitStations, this_script)
        doc.wasGeneratedBy(limitStations, get_limitStations, endTime)
        doc.wasDerivedFrom(collegeStations, limitStations, get_limitStations, get_limitStations, get_limitStations)
        doc.wasDerivedFrom(collegeTrips, limitStations, get_limitStations, get_limitStations, get_limitStations)

        repo.logout()

        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
OptimizationLimitStations.execute()
doc = OptimizationLimitStations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
