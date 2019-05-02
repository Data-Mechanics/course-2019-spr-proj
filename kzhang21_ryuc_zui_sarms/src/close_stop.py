import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import math


class close_stop(dml.Algorithm):
    contributor = 'kzhang21_ryuc_zui_sarms'
    reads = ['kzhang21_ryuc_zui_sarms.yelp_business', 'kzhang21_ryuc_zui_sarms.mbta_stops']
    writes = ['kzhang21_ryuc_zui_sarms.close_stop']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')

        yelp_business = repo['kzhang21_ryuc_zui_sarms.yelp_business']
        mbta_stops = repo['kzhang21_ryuc_zui_sarms.mbta_stops']

        df_yelp = pd.DataFrame(list(yelp_business.find()))
        df_stops = pd.DataFrame(list(mbta_stops.find()))

        # the new dataframe
        new_df = pd.DataFrame()

        for index, row in df_yelp.iterrows():
            lati_yelp = row["coordinates"]["latitude"]
            long_yelp = row["coordinates"]["longitude"]

            # saves the index and smallest distance
            min = 0
            smallest_distance = math.inf
            for i, r in df_stops.iterrows():
                lati_stop = r["attributes"]["latitude"]
                long_stop = r["attributes"]["longitude"]

                dist = math.sqrt(math.pow(lati_yelp - lati_stop, 2) + math.pow(long_yelp - long_stop, 2))

                if smallest_distance > dist:
                    min = i
                    smallest_distance = dist

            # add now the new stop
            dict = row.to_dict()
            dict.update({'closeset_stop': df_stops.iloc[min]})
            new_df = new_df.append([dict])

        repo.dropCollection("close_stop")
        repo.createCollection("close_stop")
        repo['kzhang21_ryuc_zui_sarms.close_stop'].insert_many(new_df.to_dict(orient='records'))
        repo['kzhang21_ryuc_zui_sarms.close_stop'].metadata({'complete': True})
        print(repo['kzhang21_ryuc_zui_sarms.close_stop'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:kzhang21_ryuc_zui_sarms#close_stop', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        yelp_business = doc.entity('dat:kzhang21_ryuc_zui_sarms#yelp_business', {'prov:label': 'Targeted Yelp Businesses', prov.model.PROV_TYPE: 'ont:DataSet',
                                'ont:Extension': 'json'})
        mbta_stops = doc.entity('dat:kzhang21_ryuc_zui_sarms#mbta_stops', {'prov:label': 'Nearby MBTA Stops', prov.model.PROV_TYPE: 'ont:DataSet',
                            'ont:Extension': 'json'})

        find_closest = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(find_closest, this_script)
        doc.usage(find_closest, mbta_stops, startTime, None,{prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(find_closest, yelp_business, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        yelp = doc.entity('dat:kzhang21_ryuc_zui_sarms#yelp_business',
            {prov.model.PROV_LABEL:'Filtered Yelp Dest', prov.model.PROV_TYPE:'ont:DataSet '})
        doc.wasAttributedTo(yelp, this_script)
        doc.wasGeneratedBy(yelp, find_closest, endTime)
        doc.wasDerivedFrom(yelp, yelp_business, find_closest, find_closest, find_closest)

        stop = doc.entity('dat:kzhang21_ryuc_zui_sarms#close_stop',
                                   {prov.model.PROV_LABEL: 'Filtered Close Stops', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(stop, this_script)
        doc.wasGeneratedBy(stop, find_closest, endTime)
        doc.wasDerivedFrom(stop, yelp_business, find_closest, find_closest, find_closest)

        repo.logout()

        return doc

## eof