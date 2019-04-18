from math import sin, cos, sqrt, atan2, radians
import argparse
import json
import pprint
import requests
import sys
import urllib
import csv
import dml
import prov.model
import uuid
import datetime

def csv_to_json(url):
    file = urllib.request.urlopen(url).read().decode("utf-8")  # retrieve file from datamechanics.io
    finalJson = []
    entries = file.split('\n')

    # print(entries[0])
    val = entries[0].split('\r')  # retrieve column names for keys
    
    keys = val[0].split(',')
    del val[0]
    for row in val:
        values = row.split(',')
        values[-1] = values[-1][:-1]
        dictionary = dict([(keys[i], values[i]) for i in range(len(keys))])
        finalJson.append(dictionary)

    return finalJson

def cordDist(cord1, cord2):
    #distance between two cordinates, represented in meters
    lat1 = radians(cord1[0])
    lon1 = radians(cord1[1])
    lat2 = radians(cord2[0])
    lon2 = radians(cord2[1])
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return 6373 * c * 1000 

def findMean(r):
    lines = ['Green Line B', 'Green Line C', 'Green Line D', 'Green Line E']
    total_dist = 0
    count = 0 
    for line in lines:
        #looping through all green lines
        stations = r[line]
        for station in stations:
            total_dist += station['new_location']['dist']
            count += 1
    return total_dist/count




class getNewMean(dml.Algorithm):
    contributor = 'ctrinh_fat60221_veeyn'
    reads = []
    writes = ['ctrinh_fat60221_veeyn.getNewMean']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')

        url = 'http://datamechanics.io/data/kmeans_final.csv'
        kmeansJSON = csv_to_json(url)

        url = 'http://datamechanics.io/data/min_dist_final.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        lines = ['Green Line B', 'Green Line C', 'Green Line D', 'Green Line E']
        # count = 0
        if trial == True:
            for line in lines:
                stations = r[line]
                for i in range(len(stations)):
                    stop_loc= [stations[i]['latitude'],stations[i]['longitude']]
                    min_index = 0
                    min_dist = 9999999
                    for index in range(1,len(kmeansJSON)):
                        new_loc = [float(kmeansJSON[index]['LATITUDE']),float(kmeansJSON[index]['LONGITUDE'])]
                        dist = cordDist(stop_loc,new_loc)
                        if dist < min_dist:
                            min_dist = dist
                            min_index = index
                    kmeansJSON[min_index]['dist'] = min_dist   
                    stations[i]['new_location'] = kmeansJSON[min_index]
        else:
            for line in lines:
                stations = r[line]
                for station in stations:
                    stop_loc= [station['latitude'],station['longitude']]
                    min_index = 0
                    min_dist = 9999999
                    for index in range(1,len(kmeansJSON)):
                        new_loc = [float(kmeansJSON[index]['LATITUDE']),float(kmeansJSON[index]['LONGITUDE'])]
                        dist = cordDist(stop_loc,new_loc)
                        if dist < min_dist:
                            min_dist = dist
                            min_index = index
                    kmeansJSON[min_index]['dist'] = min_dist   
                    station['new_location'] = kmeansJSON[min_index]
        newMean = findMean(r)


        repo.dropCollection("getNewMean")
        repo.createCollection("getNewMean")
        repo['ctrinh_fat60221_veeyn.getNewMean'].insert_many([{'newMEAN':newMean}])
        repo['ctrinh_fat60221_veeyn.getNewMean'].metadata({'complete':True})
        print(repo['ctrinh_fat60221_veeyn.getNewMean'].metadata())

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
        doc.add_namespace('dat', 'http://datamechanics.io/data/min_dist.csv') # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/kmeans_final.csv')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


        this_script = doc.agent('alg:ctrinh_fat60221_veeyn#getNewMean', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:min_dist.csv, ', {'prov:label':'311, data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_two = doc.entity('dat:kmeans_final.csv',{'prov:label':'311, data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        new_mean = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(new_mean, this_script)
        doc.usage(new_mean, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        doc.usage(new_mean, resource_two, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        newMean = doc.entity('dat:ctrinh_fat60221_veeyn#getMean', {prov.model.PROV_LABEL:'Mean Distance between All Station/Cafe Pairs', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(newMean, this_script)
        doc.wasGeneratedBy(newMean, new_mean, endTime)
        doc.wasDerivedFrom(newMean, resource,resource_two, new_mean, new_mean, new_mean)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.

# getNewMean.execute()


# getNewMean.execute()
# doc = getNewMean.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))