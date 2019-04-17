'''
Problem:
    We have the dataset of all car-crashing spots in revere city. After using google map to visualize these spots,
    we found that the car-crashing spots are distributed everywhere in this city. However, we still found several
    spots in the car accidents heat map where car accidents are most likely to happen, some spots like traffic circle.
    We believe that there are still other spots which we can not discover simply by observing the heat map of car
    accidents.So we decided to build up a model about this problem, using the Optimization Techniques to solve
    this problem.

    The problem is about:
        Find n spots in revere city where car accidents are most likely to happen. In this problem, n is a variable
        you can choose yourself.

    Solution:
        We abstract this real world problem to a mathematical model. To simplify the problem, we divide the map
        into grid map and calculate the car accidents happened in each grid. Then we use a slide window to
        go throw the grids and find the grid where car accidents are most likely to happen. For example n=5, then
        we have C1,C2,C3,C4,C5 for 5 positions of the slide window.

        System states S = {grid in this grid map}^n

        then the constraints are:
        1, the grid map is 100*100
        2, the slide window slides inside the region, the stride is 1 and the window size is 3, it should also be
        a square window.
        3, Ci and Cj can not overlap each other, which means if the slide window overlaps, we only keep the bigger one.

        the metric is:
        1, C = C1 +C2 + ... + C5, we need to find a state in S which maximize C

'''

import json
import numpy as np
import math
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class optimizationCrashSpot(dml.Algorithm):

    contributor = 'robinhe_rqtian_hongyf_zhjiang'
    reads = []
    writes = ['robinhe_rqtian_hongyf_zhjiang.likelyCrashSpots']

    # def readJSON(self,path):
    #     locations = []
    #     with open(path) as json_file:
    #         data = json.load(json_file)
    #     for item in data.values():
    #         if item['lon']>-71.035 and item['lon']<-70.958 and item['lat']<42.450 and item['lat']>42.390:
    #             locations.append((item['lat'],item['lon']))
    #     return locations

    def getRange(self, arr):
        max_lat = -100
        max_lon = -100
        min_lat = 100
        min_lon = 100
        for item in arr:
            if item[0] > max_lat:
                max_lat = item[0]
            if item[0] < min_lat:
                min_lat = item[0]
            if item[1] > max_lon:
                max_lon = item[1]
            if item[1] < min_lon:
                min_lon = item[1]
        range_lat = max_lat - min_lat + 0.00001
        range_lon = max_lon - min_lon + 0.00001
        return min_lat,min_lon,range_lat,range_lon

    # constraint, to build up the grid map
    def generateGrid(self, row, arr):
        grid = np.zeros((row,row))
        print(grid.shape)
        min_lat,min_lon,range_lat,range_lon = self.getRange(arr)
        unit_lon = range_lon / 100
        unit_lat = range_lat / 100
        # print(unit_lon,unit_lat)
        # lat_idx = 0
        # lon_idx = 0
        for item in arr:
            # print((item[0] - min_lat)/range_lat)
            lat_idx = math.floor((item[0] - min_lat)/unit_lat)
            lon_idx = math.floor((item[1] - min_lon)/unit_lon)
            # print(lat_idx,lon_idx,item[1],min_lon,unit_lon)
            # break
            grid[lat_idx][lon_idx] += 1
        # for i in range(0,100):
        #     print(grid[i])
        return min_lat,min_lon,unit_lat,unit_lon,grid


    # slide window algorithm
    def slide_window(self, wsize,stride,grid,n):

        maxs = [((0,0),0)]*n
        # print(maxs)
        widx = (0,0)
        for i in range(widx[0],grid.shape[0]-wsize+1,stride):
            for j in range(widx[1],grid.shape[1]-wsize+1,stride):
                wtempcount = 0
                for gi in range(0,wsize):
                    for gj in range(0,wsize):
                        # print(wtempcount,grid[i+gi][j+gj])
                        wtempcount += grid[i+gi][j+gj]
                wtemp = ((i,j),wtempcount)
                arr_temp = [wtemp]
                arr_temp.extend(maxs[1:])
                # print(arr_temp)
                if self.metric(arr_temp) > self.metric(maxs):
                    maxs = self.updateValue(wtemp,maxs)
                maxs = sorted(maxs, key=lambda item:item[1])
        # print(maxs)
        return maxs

    # update C1, C2, ..., Cn
    def updateValue(self, wtemp,maxs):
        flag = False
        for maxidx in range(0, len(maxs)):
            if self.constraintOverlap(wtemp, maxs[maxidx]):
                if wtemp[1] > maxs[maxidx][1]:
                    maxs[maxidx] = wtemp
                flag = True
                break
        if not flag:
            maxs[0] = wtemp
        return maxs

    # constraint
    def constraintOverlap(self, w1,w2):
        if abs(w1[0][0]-w2[0][0])<3 and abs(w1[0][1]-w2[0][1])<3:
            return True

    # metric
    def metric(self, arr):
        sum_arr = 0
        for item in arr:
            sum_arr += item[1]
        return sum_arr

    # translate grid index to 4 vertex of the area
    def grid2Coordiante(self, maxs, min_lat, min_lon, unit_lat, unit_lon):
        latlon = []
        for item in maxs:
            lat_lu = item[0][0] * unit_lat + min_lat
            lon_lu = item[0][1] * unit_lon + min_lon
            lat_lb = (item[0][0]+3) * unit_lat + min_lat
            lon_lb = item[0][0] * unit_lat + min_lat
            lat_ru = item[0][0] * unit_lat + min_lat
            lon_ru = (item[0][1]+3) * unit_lon + min_lon
            lat_rb = (item[0][0]+3) * unit_lat + min_lat
            lon_rb = (item[0][1]+3) * unit_lon + min_lon
            latlon.append({"left-up":(lat_lu,lon_lu),"right-up":(lat_ru,lon_ru),"right-bottom":(lat_rb,lon_rb),"left-bottom":(lat_lb,lon_lb)})
        return latlon

    @staticmethod
    def execute(n,trial=False):

        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')
        print("database seted up")
        url = 'http://datamechanics.io/data/robinhe_rqtian_hongyf_zhjiang/crashSpots.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        data = json.loads(response)
        locations = []
        for item in data.values():
            if item['lon'] > -71.035 and item['lon'] < -70.958 and item['lat'] < 42.450 and item['lat'] > 42.390:
                locations.append((item['lat'], item['lon']))

        self = optimizationCrashSpot()
        # locations = self.readJSON()
        min_lat, min_lon, unit_lat, unit_lon, grid = self.generateGrid(100, locations)
        maxs = self.slide_window(3, 1, grid, n)
        latlon = self.grid2Coordiante(maxs, min_lat, min_lon, unit_lat, unit_lon)
        for item in latlon:
            print(item)

        s = json.dumps(latlon, sort_keys=True, indent=2)
        repo.dropCollection("likelyCrashSpots")
        repo.createCollection("likelyCrashSpots")
        repo['robinhe_rqtian_hongyf_zhjiang.likelyCrashSpots'].insert_many(latlon)
        repo['robinhe_rqtian_hongyf_zhjiang.likelyCrashSpots'].metadata({'complete': True})
        print(repo['robinhe_rqtian_hongyf_zhjiang.likelyCrashSpots'].metadata())
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
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:robinhe_rqtian_hongyf_zhjiang#likelyCrashSpots',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': 'crash spots in revere city', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_likelySpots = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_likelySpots, this_script)
        doc.usage(get_likelySpots, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:projection': 'project the map to a grid map',
                   'ont:aggregation': 'calculate grids inside a slide window',
                   'ont:selection': 'select the largest n values',
                   'ont:projection': 'project back to coordinate'

                   }
                  )


        likelySpots = doc.entity('dat:robinhe_rqtian_hongyf_zhjiang#likelyCrashSpots',
                          {prov.model.PROV_LABEL: 'the n most likely car-crashing spots', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(likelySpots, this_script)
        doc.wasGeneratedBy(likelySpots, get_likelySpots, endTime)
        doc.wasDerivedFrom(likelySpots, resource, get_likelySpots, get_likelySpots, get_likelySpots)

        repo.logout()

        return doc


# optimizationCrashSpot.execute(5)
# doc = optimizationCrashSpot.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))



