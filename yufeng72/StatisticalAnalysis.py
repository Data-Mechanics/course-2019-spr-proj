import json
import dml
import prov.model
import datetime
import uuid
import scipy.stats
import numpy


class StatisticalAnalysis(dml.Algorithm):
    contributor = 'yufeng72'
    reads = ['yufeng72.tripData', 'yufeng72.tripToSchool', 'yufeng72.stationNearbySchool', 'yufeng72.idealStations']
    writes = ['yufeng72.statistics']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yufeng72', 'yufeng72')

        # statistical analysis

        trips = repo['yufeng72.tripData'].find()
        tripData = []
        for i in trips:
            tripData.append(i)

        # age statistics
        ageCount = []
        for year in range(1917, 2017):
            newRow = {'Age': 2017 - year, 'Count': 0}
            ageCount.append(newRow)

        for i in tripData:
            for age in ageCount:
                if age['Age'] == 2017 - int(i['birth year']):
                    age['Count'] = age['Count'] + 1
                    break

        ageCount.reverse();

        # for age in ageCount:
        #     print(age)

        countStudents = 0
        countAllPeople = 0
        for age in ageCount:
            countAllPeople = countAllPeople + age['Count']
            if 17 < age['Age'] < 25:
                countStudents = countAllPeople + age['Count']
        studentProportion = countStudents / countAllPeople
        print("Proportion of College Students:", studentProportion)

        # membership statistics
        membershipCount = []
        membershipCount.append({'Usertype': 'Subscriber', 'Count': 0})
        membershipCount.append({'Usertype': 'Customer', 'Count': 0})

        for i in tripData:
            if i['usertype'] == 'Subscriber':
                membershipCount[0]['Count'] = membershipCount[0]['Count'] + 1
            else:
                membershipCount[1]['Count'] = membershipCount[1]['Count'] + 1

        # for membership in membershipCount:
        #     print(membership)

        countMembers = int(membershipCount[0]['Count'])
        countAllCustomers = int(membershipCount[0]['Count']) + int(membershipCount[1]['Count'])
        subscriberProportion = countMembers / countAllCustomers
        print("Proportion of Subscribed Member:", subscriberProportion)

        # relation between age and membership
        tripAge = []
        for i in tripData:
            tripAge.append(2017 - int(i['birth year']))

        tripMembership = []
        for i in tripData:
            if i['usertype'] == 'Subscriber':
                tripMembership.append(1)
            else:
                tripMembership.append(0)

        std_Age = numpy.std(tripAge, ddof=1)
        std_IsMembership = numpy.std(tripMembership, ddof=1)
        cc_Age_IsMembership, pv_Age_IsMembership = scipy.stats.pearsonr(tripAge, tripMembership)
        print("Correlation Coefficient of Age and Membership:", cc_Age_IsMembership)

        # relation between station number, trip number and student number
        tripsToSchool = repo['yufeng72.tripToSchool'].find()
        collegeTrips = []
        for i in tripsToSchool:
            collegeTrips.append(i)
        tripNumber = []
        for i in collegeTrips:
            tripNumber.append(int(i['TripToSchool']))
        studentNumber = []
        for i in collegeTrips:
            studentNumber.append(int(i['NumStudent']))

        stationsNearbySchool = repo['yufeng72.stationNearbySchool'].find()
        collegeStations = []
        for i in stationsNearbySchool:
            collegeStations.append(i)
        stationNumber = []
        for i in collegeStations:
            stationNumber.append(i['HubwayStationNearby'])

        idealStationsNearbySchool = repo['yufeng72.idealStations'].find()
        idealCollegeStations = []
        for i in idealStationsNearbySchool:
            idealCollegeStations.append(i)
        idealStationNumber = []
        for i in idealCollegeStations:
            idealStationNumber.append(i['HubwayStationNearby'])

        std_TripNum = numpy.std(tripNumber, ddof=1)
        std_StudentNum = numpy.std(studentNumber, ddof=1)
        std_StationNum = numpy.std(stationNumber, ddof=1)
        std_IdealStationNum = numpy.std(idealStationNumber, ddof=1)
        # print(std_TripNum)
        # print(std_StudentNum)
        # print(std_StationNum)
        # print(std_IdealStationNum)

        cc_TripNum_StudentNum, pv_TripNum_StudentNum = scipy.stats.pearsonr(tripNumber, studentNumber)
        cc_TripNum_StationNum, pv_TripNum_StationNum = scipy.stats.pearsonr(tripNumber, stationNumber)
        cc_TripNum_IdealStationNum, pv_TripNum_IdealStationNum = scipy.stats.pearsonr(tripNumber, idealStationNumber)
        cc_StudentNum_StationNum, pv_StudentNum_StationNum = scipy.stats.pearsonr(studentNumber, stationNumber)
        # print("Correlation Coefficient of TripNum and StudentNum:", cc_TripNum_StudentNum)
        print("Correlation Coefficient of TripNum and StationNum:", cc_TripNum_StationNum)
        print("Correlation Coefficient of TripNum and IdealStationNum:", cc_TripNum_IdealStationNum)
        # print("Correlation Coefficient of StudentNum and StationNum:", cc_StudentNum_StationNum)

        print()

        result = {}
        result['User Student Proportion'] = studentProportion
        result['User Subscriber Proportion'] = subscriberProportion
        result['STD of User Age'] = std_Age
        result['STD of User Membership'] = std_IsMembership
        result['Correlation Coefficient of Age and Membership'] = cc_Age_IsMembership

        result['STD of TripNum'] = std_TripNum
        result['STD of StudentNum'] = std_StudentNum
        result['STD of StationNum'] = std_StationNum
        result['STD of IdealStationNum'] = std_IdealStationNum
        result['Correlation Coefficient of TripNum and StudentNum'] = cc_TripNum_StudentNum
        result['Correlation Coefficient of TripNum and StationNum'] = cc_TripNum_StationNum
        result['Correlation Coefficient of TripNum and IdealStationNum'] = cc_TripNum_IdealStationNum
        result['Correlation Coefficient of StudentNum and StationNum'] = cc_StudentNum_StationNum

        repo.dropCollection('statistics')
        repo.createCollection('statistics')
        repo['yufeng72.statistics'].insert_one(result)
        repo['yufeng72.statistics'].metadata({'complete': True})
        print(repo['yufeng72.statistics'].metadata())

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

        this_script = doc.agent('alg:yufeng72#StatisticalAnalysis',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        tripData = doc.entity('dat:yufeng72#tripData',
                              {prov.model.PROV_LABEL: 'Trip Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        tripToSchool = doc.entity('dat:yufeng72#tripToSchool',
                                  {prov.model.PROV_LABEL: 'Trips End at Colleges', prov.model.PROV_TYPE: 'ont:DataSet'})

        assignedStations = doc.entity('dat:yufeng72#stationNearbySchool',
                                      {prov.model.PROV_LABEL: 'Stations Nearby Colleges',
                                       prov.model.PROV_TYPE: 'ont:DataSet'})

        idealStations = doc.entity('dat:yufeng72#idealStations',
                                   {prov.model.PROV_LABEL: 'Ideal Number of Stations for Colleges',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})

        get_statistics = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_statistics, this_script)
        doc.used(get_statistics, tripData, startTime)
        doc.used(get_statistics, tripToSchool, startTime)
        doc.used(get_statistics, assignedStations, startTime)
        doc.used(get_statistics, idealStations, startTime)

        statistics = doc.entity('dat:yufeng72#statistics',
                                {prov.model.PROV_LABEL: 'Statistical Analysis',
                                 prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(statistics, this_script)
        doc.wasGeneratedBy(statistics, get_statistics, endTime)
        doc.wasDerivedFrom(tripData, statistics, get_statistics, get_statistics, get_statistics)
        doc.wasDerivedFrom(tripToSchool, statistics, get_statistics, get_statistics, get_statistics)
        doc.wasDerivedFrom(assignedStations, statistics, get_statistics, get_statistics, get_statistics)
        doc.wasDerivedFrom(idealStations, statistics, get_statistics, get_statistics, get_statistics)

        repo.logout()

        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
StatisticalAnalysis.execute()
doc = StatisticalAnalysis.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
