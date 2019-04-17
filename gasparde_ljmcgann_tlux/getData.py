import datetime
import json
import urllib.request
import uuid

import dml
import prov.model


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
        repo[getData.contributor + ".CensusTractShape"].metadata({'complete': True})

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

        # Parcels with their assessment value and type
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

        # this loop is to remove duplicates that would cause problems inserting into mongo
        ids = set()
        # print(All_Assessments)
        unique_pid = []
        for assess in All_Assessments:
            if assess["PID"] not in ids:
                ids.add(assess["PID"])
                unique_pid.append({"_id": assess["PID"], "AV_TOTAL": assess["AV_TOTAL"], "PTYPE": assess["PTYPE"],
                                   "LAND_SF": assess["LAND_SF"]})

        # print(unique_pid)
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
        # print(open_spaces)
        open_spaces = [i for i in open_spaces if i['properties']['TypeLong'] in wanted_types]
        repo.dropCollection(getData.contributor + ".OpenSpaces")
        repo.createCollection(getData.contributor + ".OpenSpaces")
        repo[getData.contributor + ".OpenSpaces"].insert_many(open_spaces)
        repo[getData.contributor + ".OpenSpaces"].metadata({'complete': True})

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:gasparde_ljmcgann_tlux#getData',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'],
                                 'ont:Extension': 'py'})

        # Data Mechanics Portal

        doc.add_namespace('bct', 'http://datamechanics.io/data/gasparde_ljmcgann_tlux/')

        bct_resource = doc.entity('bct:boston_census_track',
                                  {'prov:label': 'Collect Boston Census Tract Shapes',
                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'json'})
        getCensusTractShape = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(getCensusTractShape, this_script)
        doc.usage(getCensusTractShape, bct_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        CensusTractShape = doc.entity('dat:gasparde_ljmcgann_tlux#CensusTractShape',
                                      {prov.model.PROV_LABEL: 'Boston Census Tract Shape',
                                       prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(CensusTractShape, this_script)
        doc.wasGeneratedBy(CensusTractShape, getCensusTractShape, endTime)
        doc.wasDerivedFrom(CensusTractShape, bct_resource, getCensusTractShape, getCensusTractShape,
                           getCensusTractShape)

        # CDC Portal

        doc.add_namespace('cdc', 'https://chronicdata.cdc.gov/resource/')

        cth_resource = doc.entity('cdc:47z2-4wuh',
                                  {'prov:label': 'Collect Boston Census Tract Health Statistics',
                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'json'})
        getCensusTractHealth = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getCensusTractHealth, this_script)
        doc.usage(getCensusTractHealth, cth_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query': '?placename=Boston'})

        CensusTractHealth = doc.entity('dat:gasparde_ljmcgann_tlux#CensusTractHealth',
                                       {prov.model.PROV_LABEL: 'Boston Census Tract Health Statistics',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(CensusTractHealth, this_script)
        doc.wasGeneratedBy(CensusTractHealth, getCensusTractHealth, endTime)
        doc.wasDerivedFrom(CensusTractHealth, cth_resource, getCensusTractHealth, getCensusTractHealth,
                           getCensusTractHealth)

        # Boston Open Data

        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        osb_resource = doc.entity('bod:2868d370c55d4d458d4ae2224ef8cddd_7',
                                  {'prov:label': 'Collect Open Spaces in Boston',
                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'geojson'})

        getOpenSpaces = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getOpenSpaces, this_script)
        doc.usage(getOpenSpaces, osb_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        OpenSpaces = doc.entity('dat:gasparde_ljmcgann_tlux#OpenSpaces',
                                {prov.model.PROV_LABEL: 'Open Spaces in Boston that can be Parks',
                                 prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(OpenSpaces, this_script)
        doc.wasGeneratedBy(OpenSpaces, getOpenSpaces, endTime)
        doc.wasDerivedFrom(OpenSpaces, osb_resource, getOpenSpaces, getOpenSpaces, getOpenSpaces)

        bnh_resource = doc.entity('bod:3525b0ee6e6b427f9aab5d0a1d0a1a28_0',
                                  {'prov:label': 'Collect Boston Neighborhoods',
                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'geojson'})

        get_Neighborhoods = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Neighborhoods, this_script)
        doc.usage(get_Neighborhoods, bnh_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        Neighborhoods = doc.entity('dat:gasparde_ljmcgann_tlux#Neighborhoods',
                                   {prov.model.PROV_LABEL: 'Shape of Boston Neighborhoods',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(Neighborhoods, this_script)
        doc.wasGeneratedBy(Neighborhoods, get_Neighborhoods, endTime)
        doc.wasDerivedFrom(Neighborhoods, bnh_resource, get_Neighborhoods, get_Neighborhoods, get_Neighborhoods)

        pgd_resource = doc.entity('bod:b7739e6673104c048f5e2f28bb9b2281_0',
                                  {'prov:label': 'Collect Parcel Shape',
                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'geojson'})

        get_ParcelGeo = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_ParcelGeo, this_script)
        doc.usage(get_ParcelGeo, pgd_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        ParcelGeo = doc.entity('dat:gasparde_ljmcgann_tlux#ParcelGeo',
                               {prov.model.PROV_LABEL: 'The Shape of the Parcels', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(ParcelGeo, this_script)
        doc.wasGeneratedBy(ParcelGeo, get_ParcelGeo, endTime)
        doc.wasDerivedFrom(ParcelGeo, pgd_resource, get_ParcelGeo, get_ParcelGeo, get_ParcelGeo)

        pas_resource = doc.entity('bod:fd351943-c2c6-4630-992d-3f895360febd',
                                  {'prov:label': 'Collect Boston Parcel Assessment Value',
                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'json'})
        get_ParcelAssessments = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_ParcelAssessments, this_script)
        doc.usage(get_ParcelAssessments, pas_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        ParcelAssessments = doc.entity('dat:gasparde_ljmcgann_tlux#ParcelAssessments',
                                       {prov.model.PROV_LABEL: 'Assessment Value of Parcels',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(ParcelAssessments, this_script)
        doc.wasGeneratedBy(ParcelAssessments, get_ParcelAssessments, endTime)
        doc.wasDerivedFrom(ParcelAssessments, pas_resource, get_ParcelAssessments, get_ParcelAssessments,
                           get_ParcelAssessments)
        return doc
