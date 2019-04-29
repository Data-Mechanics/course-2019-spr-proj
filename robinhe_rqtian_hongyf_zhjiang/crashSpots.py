import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import json
from pyproj import Proj, transform

class crashSpots(dml.Algorithm):
    contributor = 'robinhe_rqtian_hongyf_zhjiang'
    reads = []
    writes = ['robinhe_rqtian_hongyf_zhjiang.crashSpots']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')
        print("database seted up")
        url = 'http://datamechanics.io/data/robinhe_rqtian_hongyf_zhjiang/crash_data_01_19.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        data = json.loads(response)
        # json_file =open('./crash_data_01_19.json')
        # data = json.load(json_file)
        print("data loaded, transfering latitude and longitude...")
        crash_col = []
        inProj = Proj(init='epsg:2805')
        outProj = Proj(init='epsg:4152')
        js_arr = []
        finishcount = 0
        for item in data:
            finishcount += 1
            if finishcount % 1000 == 0:
                print("finished " + str(finishcount) + "rows")
            if item["City/Town"] != "REVERE":
                continue
            # print(item)
            if item['X'] == '':
                continue
            x1, y1 = item['X'], item['Y']
            x2, y2 = transform(inProj, outProj, x1, y1)
            if not (x2>-71.035 and x2<-70.958 and y2<42.450 and y2>42.390):
                continue
            arr = item['Crash Date'].split('/')
            date = datetime.datetime(2000 + int(arr[2]), int(arr[0]), int(arr[1]))
            date = date.strftime("%Y-%m-%d")
            # print(date, item['Crash Date'])
            try:
                time = datetime.datetime.strptime(item['Crash Time'], '%I:%M %p')
                time = time.strftime('%H:%M:%S')
                # print(time, item['Crash Time'])
            except ValueError:
                continue
            crash_info = {"date": date + " " + time, "lat": y2, "lon": x2}
            js_arr.append('new google.maps.LatLng(' + str(round(y2, 6)) + ', ' + str(round(x2, 6)) + '),\n')
            if "Crash Number" in item.keys():
                crash_col.append(crash_info)
            elif 'RMV Crash Number' in item.keys():
                crash_col.append(crash_info)

        print("contains " + str(len(crash_col)) + "crashes");


        s = json.dumps(crash_col, sort_keys=True, indent=2)
        repo.dropCollection("crashSpots")
        repo.createCollection("crashSpots")
        repo['robinhe_rqtian_hongyf_zhjiang.crashSpots'].insert_many(crash_col)
        repo['robinhe_rqtian_hongyf_zhjiang.crashSpots'].metadata({'complete': True})
        print(repo['robinhe_rqtian_hongyf_zhjiang.crashSpots'].metadata())
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
        repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:robinhe_rqtian_hongyf_zhjiang#crashSpots',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': 'crash spots in PROJCS format', prov.model.PROV_TYPE: 'ont:DataResource'})
        get_spots = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_spots, this_script)
        doc.usage(get_spots, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Selection': 'select validate ones',
                   'ont:Projection':'project PROJCS to GEOGCS'
                   }
                  )

        crash_spots = doc.entity('dat:robinhe_rqtian_hongyf_zhjiang#crashSpots',
                          {prov.model.PROV_LABEL: 'transform PROJCS to GEOGCS', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash_spots, this_script)
        doc.wasGeneratedBy(crash_spots, get_spots, endTime)
        doc.wasDerivedFrom(crash_spots, resource, get_spots, get_spots, get_spots)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# crashSpots.execute()
# doc = crashSpots.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof