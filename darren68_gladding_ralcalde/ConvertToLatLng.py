import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io
from pyproj import Proj, transform

class ConvertToLatLng(dml.Algorithm):
    contributor = 'darren68_gladding_ralcalde'
    reads = ['darren68_gladding_ralcalde.Revere2001to2016']
    writes = ['darren68_gladding_ralcalde.Revere2001to2016LatLng']

    @staticmethod
    def execute(trial=False):
        '''Retrieve data set of crashes in Revere for the year 2016 and store it in Mongo'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

        collection = repo['darren68_gladding_ralcalde.Revere2001to2016']
        repo.dropCollection("Revere2001to2016LatLng")
        repo.createCollection("Revere2001to2016LatLng")


        state_plane = Proj(init='EPSG:26986', preserve_units=True)
        wgs = Proj(proj='latlong', datum='WGS84', ellps='WGS84')
        i = 1
        for doc in collection.find():
            if len(doc['x']) != 0 and len(doc['y']) != 0:
                x1 = doc.pop('x', None)
                y1 = doc.pop('y', None)

                try:
                    lng, lat = transform(state_plane, wgs, float(x1), float(y1))
                except:
                    continue

                doc['longtitude'] = lng
                doc['latitude'] = lat

                repo['darren68_gladding_ralcalde.Revere2001to2016LatLng'].insert(doc)
                print(i)
            i += 1


        repo['darren68_gladding_ralcalde.Revere2001to2016LatLng'].metadata({'complete': True})

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


        # Agent, entity, activity
        this_script = doc.agent('alg:darren68_gladding_ralcalde#ConvertToLatLng',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})


        Revere_data = doc.entity('dat:darren68_gladding_ralcalde#Revere2001to2016',
                              {'prov:label': "Revere data from 2001 to 2016", prov.model.PROV_TYPE: 'ont:DataSet',
                               'ont:Extension': 'csv'})


        # Activity
        get_RevereData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_RevereData, this_script)

        doc.usage(get_RevereData, Revere_data, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   })

        LatLngDataSet = doc.entity('dat:darren68_gladding_ralcalde#Revere2001to2016LatLng',
                             {prov.model.PROV_LABEL: "Holds state plane coordinate data converted to lat/lng"
                                 , prov.model.PROV_TYPE: 'ont:DataSet'})

        createLatLngData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(createLatLngData, this_script)
        doc.wasAttributedTo(LatLngDataSet, this_script)
        doc.wasGeneratedBy(LatLngDataSet, createLatLngData, endTime)
        doc.usage(createLatLngData, Revere_data, startTime, None,
                  {prov.model.PROV_LABEL: "Used accident data from all the towns to compute rates of change in accidents"
                      , prov.model.PROV_TYPE: 'ont:Computation'})


        doc.wasDerivedFrom(LatLngDataSet, Revere_data, createLatLngData, createLatLngData, createLatLngData)
        return doc
