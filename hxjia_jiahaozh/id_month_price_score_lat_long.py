import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd


class Id_month_price_score_lat_long(dml.Algorithm):
    contributor = 'hxjia_jiahaozh'
    reads = ['hxjia_jiahaozh.listings', 'hxjia_jiahaozh.calendar']
    writes = ['hxjia_jiahaozh.id_month_price_score_lat_long']

    @staticmethod
    def execute(trial=False):

        def project(R, p):
            return [p(t) for t in R]

        def select(R, s):
            return [t for t in R if s(t)]

        def product(R, S):
            return [(t, u) for t in R for u in S]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [[key, f([v for (k, v) in R if k == key])] for key in keys]

        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hxjia_jiahaozh', 'hxjia_jiahaozh')

        collection_listings = repo.hxjia_jiahaozh.listings
        listings = collection_listings.find({})
        collection_calendar = repo.hxjia_jiahaozh.calendar
        calendar = collection_calendar.find({})

        listings_data = []
        calendar_data = []


        for data in listings:
            if data['review_scores_rating']:

                listings_data.append(data)

        # get all the ids in listings_data
        id_data = project(listings_data, lambda t: t['id'])

        # get the data in calendar that is in listings_data
        for data in calendar:
            if data['price'] and data['listing_id'] in id_data:
                calendar_data.append(data)

        calendar_data = project(calendar_data, lambda t: [t['listing_id'], t['price'].replace("$", "").replace(",", "")])

        for i in calendar_data:
            i[1] = float(i[1])

        # calculate the the mean price for each id
        mean = aggregate(calendar_data, lambda t: sum(t) / len(t))

        # combine the other information needed in listing_data with the id_meanprice data
        for h in listings_data:
            for t in mean:
                if t[0] == h['id']:
                    t.append(h['review_scores_rating'])
                    t.append(h['number_of_reviews'])
                    t.append(h['latitude'])
                    t.append(h['longitude'])

        result = project(mean, lambda t: {'id': t[0], 'price': t[1], 'review_score': t[2],'number_of_reviews': t[3], 'latitude': t[4], 'longitude': t[5]})
#        print(result)

#
        # r = json.loads(new_bl.to_json(orient='records'))
        # s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("id_month_price_score_lat_long")
        repo.createCollection("id_month_price_score_lat_long")
        repo['hxjia_jiahaozh.id_month_price_score_lat_long'].insert_many(result)
        repo['hxjia_jiahaozh.id_month_price_score_lat_long'].metadata({'complete': True})
        print(repo['hxjia_jiahaozh.id_month_price_score_lat_long'].metadata())

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
        repo.authenticate('hxjia_jiahaozh', 'hxjia_jiahaozh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/hxjia_jiahaozh/')

        this_script = doc.agent('alg:hxjia_jiahaozh#id_month_price_score_lat_long',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource1 = doc.entity('bdp:calendar',
                              {'prov:label': 'calendar, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        resource2 = doc.entity('bdp:listings',
                              {'prov:label': 'listings, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        transformation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transformation, this_script)
        doc.usage(transformation, resource1, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Transformation',
                   'ont:Query': '?type=calendar&$select=listing_id, price'
                   }
                  )
        doc.usage(transformation, resource2, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Transformation',
                   'ont:Query': '?type=&$select=id, review_scores_rating, number_of_reviews latitude, longitude'
                   }
                  )

        idmonthpricescorelatlong = doc.entity('dat:hxjia_jiahaozh#id_month_price_score_lat_long',
                          {prov.model.PROV_LABEL: 'id month price score lat long', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(idmonthpricescorelatlong, this_script)
        doc.wasGeneratedBy(idmonthpricescorelatlong, transformation, endTime)
        doc.wasDerivedFrom(idmonthpricescorelatlong, resource1, transformation, transformation, transformation)
        doc.wasDerivedFrom(idmonthpricescorelatlong, resource2, transformation, transformation, transformation)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# Id_month_price_score_lat_long.execute()
# doc = Id_month_price_score_lat_long.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

