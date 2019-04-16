import urllib.request
import json
from shapely.geometry import Polygon
import json
import dml
import prov.model
import datetime
import csv
import codecs
import uuid

class getData(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = []
    writes = [contributor + ".CensusTractShape", contributor + ".CensusTractHealth",
              contributor + ".Neighborhoods", contributor + ".ParcelAssessments",
              contributor + ".ParcelGeo", contributor + ".OpenSpaces"]

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        name = "gasparde_ljmcgann_tlux"
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(getData.contributor, getData.contributor)

        ######################################################

        # census tract ids with their associated geojson shape
        url = 'http://datamechanics.io/data/gasparde_ljmcgann_tlux/boston_census_track.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        censusTract = json.loads(response)
        repo.dropCollection(getData.contributor + ".CensusTractShape")
        repo.createCollection(getData.contributor + ".CensusTractShape")
        repo[getData.contributor + ".CensusTractShape"].insert_many(censusTract)
        repo[getData.contributor + ".CensusTractShape"].metadata({'complete':True})

        #######################################################

        # CDC data of various health metrics within each census tract
        url = 'https://chronicdata.cdc.gov/resource/47z2-4wuh.json?placename=Boston'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        cdcData = json.loads(response)
        # print(cdcData)
        # only want to keep these three statistics
        for i in range(len(cdcData)):
            d = {"_id": cdcData[i]["tractfips"],
                 "obesity": cdcData[i]["obesity_crudeprev"],
                 "low_phys": cdcData[i]["lpa_crudeprev"],
                 "asthma": cdcData[i]["casthma_crudeprev"]}
            cdcData[i] = d
        repo.dropCollection(getData.contributor + ".CensusTractHealth")
        repo.createCollection(getData.contributor + ".CensusTractHealth")
        repo[getData.contributor + ".CensusTractHealth"].insert_many(cdcData)
        repo[getData.contributor + ".CensusTractHealth"].metadata({'complete': True})

        #########################################################

        # Boston neighborhoods and there associated geojson shapes
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        neighborhoods = json.loads(response)
        repo.dropCollection(getData.contributor + ".Neighborhoods")
        repo.createCollection(getData.contributor + ".Neighborhoods")
        repo[getData.contributor + ".Neighborhoods"].insert_many(neighborhoods["features"])
        repo[getData.contributor + ".Neighborhoods"].metadata({'complete': True})

        ##########################################################

        #Parcels with their assessment value and type
        All_Assessments = []
        for i in range(9):
            # need to iterate because we api request only brings max 20000 parcels
            skip = 20000 * (i)
            url1 = "https://data.boston.gov/datastore/odata3.0/fd351943-c2c6-4630-992d-3f895360febd?$top=20000&$format=json&$skip=" + str(
                skip)
            response = urllib.request.urlopen(url1).read()
            Assessment = json.loads(response)
            Assessment = Assessment['value']
            All_Assessments += Assessment
        ids = set()
        #print(All_Assessments)
        unique_pid =[]
        # this loop is to remove duplicates that allow for us to insert into mongo
        for assess in All_Assessments:
            if assess["PID"] not in ids:
                ids.add(assess["PID"])
                unique_pid.append({"_id":assess["PID"], "AV_TOTAL": assess["AV_TOTAL"], "PTYPE": assess["PTYPE"],
                                   "LAND_SF":assess["LAND_SF"]})

        #print(unique_pid)
        repo.dropCollection(getData.contributor + ".ParcelAssessments")
        repo.createCollection(getData.contributor + ".ParcelAssessments")
        repo[getData.contributor + ".ParcelAssessments"].insert_many(unique_pid)

        repo[getData.contributor + ".ParcelAssessments"].metadata({'complete': True})

        ##########################################################

        # parcel geojson data
        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/b7739e6673104c048f5e2f28bb9b2281_0.geojson"

        response = urllib.request.urlopen(url).read()

        parcelGeo = json.loads(response.decode('utf-8'))["features"]
        repo.dropCollection(getData.contributor + ".ParcelGeo")
        repo.createCollection(getData.contributor + ".ParcelGeo")
        repo[getData.contributor + ".ParcelGeo"].insert_many(parcelGeo)
        repo[getData.contributor + ".ParcelGeo"].metadata({'complete': True})

        ###########################################################

        # open spaces in boston
        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson"
        wanted_types = ["Parkways, Reservations & Beaches", "Parks, Playgrounds & Athletic Fields",
                        "Urban Wilds & Natural Areas", "Community Gardens"]
        open_spaces = json.loads(urllib.request.urlopen(url).read())["features"]
        #print(open_spaces)
        open_spaces = [i for i in open_spaces if i['properties']['TypeLong'] in wanted_types]
        repo.dropCollection(getData.contributor + ".OpenSpaces")
        repo.createCollection(getData.contributor + ".OpenSpaces")
        repo[getData.contributor + ".OpenSpaces"].insert_many(open_spaces)
        repo[getData.contributor + ".OpenSpaces"].metadata({'complete': True})

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gasparde_ljmcgann_tlux', 'gasparde_ljmcgann_tlux')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:gasparde_ljmcgann_tlux#collect', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': 'Collect Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})



        get_CensusTractShape = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_CensusTractHealth = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_Neighborhoods = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_ParcelAssessments = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_ParcelGeo = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_OpenSpaces = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)


        doc.wasAssociatedWith(get_CensusTractHealth, this_script)
        doc.wasAssociatedWith(get_CensusTractShape, this_script)
        doc.wasAssociatedWith(get_Neighborhoods, this_script)
        doc.wasAssociatedWith(get_ParcelGeo, this_script)
        doc.wasAssociatedWith(get_ParcelAssessments, this_script)
        doc.wasAssociatedWith(get_OpenSpaces, this_script)


        doc.usage(get_CensusTractShape, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_CensusTractHealth, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_OpenSpaces, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_ParcelGeo, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_Neighborhoods, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_ParcelAssessments, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )


        CensusTractShape = doc.entity('dat:alice_bob#lost',
                          {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(CensusTractShape, this_script)
        doc.wasGeneratedBy(CensusTractShape, get_CensusTractShape, endTime)
        doc.wasDerivedFrom(CensusTractShape, resource, get_CensusTractShape, get_CensusTractShape, get_CensusTractShape)

        CensusTractHealth = doc.entity('dat:alice_bob#found',
                           {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(CensusTractHealth, this_script)
        doc.wasGeneratedBy(CensusTractHealth, get_CensusTractHealth, endTime)
        doc.wasDerivedFrom(CensusTractHealth, resource, get_CensusTractHealth, get_CensusTractHealth, get_CensusTractHealth)

        OpenSpaces = doc.entity('dat:alice_bob#lost',
                          {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(OpenSpaces, this_script)
        doc.wasGeneratedBy(OpenSpaces, get_OpenSpaces, endTime)
        doc.wasDerivedFrom(OpenSpaces, resource, get_OpenSpaces, get_OpenSpaces, get_OpenSpaces)

        ParcelGeo = doc.entity('dat:alice_bob#found',
                           {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(ParcelGeo, this_script)
        doc.wasGeneratedBy(ParcelGeo, get_ParcelGeo, endTime)
        doc.wasDerivedFrom(ParcelGeo, resource, get_ParcelGeo, get_ParcelGeo, get_ParcelGeo)

        Neighborhoods = doc.entity('dat:alice_bob#lost',
                          {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(Neighborhoods, this_script)
        doc.wasGeneratedBy(Neighborhoods, get_Neighborhoods, endTime)
        doc.wasDerivedFrom(Neighborhoods, resource, get_Neighborhoods, get_Neighborhoods, get_Neighborhoods)

        ParcelAssessments = doc.entity('dat:alice_bob#found',
                           {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(ParcelAssessments, this_script)
        doc.wasGeneratedBy(ParcelAssessments, get_ParcelAssessments, endTime)
        doc.wasDerivedFrom(ParcelAssessments, resource, get_ParcelAssessments, get_ParcelAssessments, get_ParcelAssessments)

        repo.logout()

        return doc



getData.execute()

