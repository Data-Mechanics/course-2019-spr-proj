#!/usr/bin/env python
# coding: utf-8
import shapefile
import shapely.geometry as geometry
import math
import pandas as pd
import dml
import prov.model
import datetime
import uuid
import json

class AIRBNB_statistics(dml.Algorithm):

    contributor = "xcao19_yjhang_zy0105"
    reads = []
    writes = ['Jinghang_Yuan.AIRBNB_statistics']

    @staticmethod
    def execute(trial = False):

        # get airbnb listings
        df = pd.read_csv('http://data.insideairbnb.com/united-states/ma/boston/2019-01-17/visualisations/listings.csv')
        df = df.rename({'neighbourhood': 'neighborhood'}, axis='columns')

        # clean up data
        for i, row in df.iterrows():
            if df.at[i, "neighborhood"] == "Longwood Medical Area":
                df.at[i, "neighborhood"] = "Longwood"

        # Average price per neighborhood
        np = df[['neighborhood', 'price', 'longitude', 'latitude']]
        np = np.groupby(['neighborhood']).mean()

        #load in crimes with neighborhood data
        cdf = pd.read_csv('http://datamechanics.io/data/crimes_with_neighborhood.csv')
        cdf_n = (cdf.groupby(['neighborhood']).size()).to_frame()
        cdf_n = cdf_n.rename({0: 'crime_rate'}, axis='columns')
        cdf_n

        #merge data and export to csv and into mongo
        merged = (cdf_n.join(np, on='neighborhood')).dropna()
        exp = merged.to_csv('airbnb_neighborhood_crime_rate.csv')

        json_df = df.to_json(orient='records')
        r = json.loads(json_df)

        repo.dropCollection("AIRBNB_statistics")
        repo.createCollection("AIRBNB_statistics")
        repo['Jinghang_Yuan.AIRBNB_statistics'].insert_many(r)
        repo['Jinghang_Yuan.AIRBNB_statistics'].metadata({'complete':True})

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xcao19', 'xcao19')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset')
        doc.add_namespace('bnb', 'http://insideairbnb.com/get-the-data.html')

        #Entities
        bnb = doc.entity('bnb: listings.csv',
                {prov.model.PROV_TYPE: 'ont:DataResource','ont: Extension': 'csv'})
        crime = doc.entity('dat: crimes_with_neighborhood.csv', {prov.model.PROV_LABEL: 'crime data', prov.model.PROV_TYPE: 'ont: DataSet'})

        AIRBNB_statistics = doc.entity('dat: xcao19_yjhang_zy0105.AIRBNB_statistics', {prov.model.PROV_LABEL: 'merged airbnb and crime data', prov.model.PROV_TYPE: 'ont: DataSet'})

        #Agents
        this_script = doc.agent('alg: xcao19_yjhang_zy0105#AIRBNB_statistics.py',
                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': '.py'})

        #Algos/Activities
        merge_resources = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        #Prov
        doc.wasAssociatedWith(merge_resources, this_script)
        doc.wasAttributedTo(AIRBNB_statistics, this_script)
        doc.wasGeneratedBy(AIRBNB_statistics, merge_resources, endTime)
        doc.usage(merge_resources, bnb, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': 'id,name,host_id,host_name,neighbourhood_group,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,last_review,reviews_per_month,calculated_host_listings_count,availability_365'
                   }
                  )
        doc.usage(merge_resources, crime, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': 'incident_number,offense_code,offence_code_group,offence_description,district,reporting_area,shooting,occured_on_date,year,month,day_of_week,hour,ucr_part,street,lat,long,location'
                   }
                  )
        doc.wasDerivedFrom(AIRBNB_statistics, bnb, merge_resources, merge_resources, merge_resources)
        doc.wasDerivedFrom(AIRBNB_statistics, crime, merge_resources, merge_resources, merge_resources)
        repo.logout()

        return doc

doc = AIRBNB_statistics.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))