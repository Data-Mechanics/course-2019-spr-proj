"""
Pulling data from City of Boston data portal about
[Food Establishment Inspection](https://data.boston.gov/dataset/food-establishment-inspections)

# Resource ID: 4582bec6-2b4f-4f9e-bc55-cbaa73117f4c

"""
import datetime
import io
import logging
import os
import uuid
import zipfile

import dml
import pandas as pd
import prov.model
import requests

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

URL = "https://data.boston.gov/dataset/03693648-2c62-4a2c-a4ec-48de2ee14e18/resource/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c/download/tmp77velm71.csv"


class FoodInspections(dml.Algorithm):
    contributor = 'kzhang21_ryuc_zui_sarms'
    reads = []
    writes = ['kzhang21_ryuc_zui_sarms.food_inspections', "kzhang21_ryuc_zui_sarms.food_inspections_squished"]

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        print("Welcome to ", __name__)
        # Set up the database connection.
        # This will fail to connect to the one require SSH auth
        client = dml.pymongo.MongoClient()
        repo = client.repo

        log.debug("Authenticating into mongoDB")
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')

        url = 'http://datamechanics.io/data/food_inspections.zip'
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        file_path = os.path.dirname(os.path.realpath(__file__))
        f = z.read("tmp77velm71.csv")
        csv_file = io.BytesIO(f)
        log.debug("Extracting file ")
        DF = pd.read_csv(csv_file, low_memory=False)

        if trial:
            log.info("In trial mode, only fetch 20k")
            DF = DF.iloc[:20000, :]

        # (DF,_,_) = pd.read_csv(url, low_memory=False)

        log.debug("Fetching CSV from %s", 'tmp77velm71')

        # Project to select only the column we wants
        selected_columns = ["businessname", "licenseno", "violstatus", "address", "city", "state", "zip", "property_id",
                            "location", "violdttm", "violation"]

        # Remove the row which has an empty violation date.
        DF = DF.iloc[DF["violdttm"].dropna().index, :].loc[:, selected_columns]

        DF["_id"] = DF.index.values

        r_dict = DF.to_dict(orient="record")

        repo.dropCollection("food_inspections")
        repo.createCollection("food_inspections")

        repo['kzhang21_ryuc_zui_sarms.food_inspections'].insert_many(r_dict)

        log.debug("Finished with the non-squished version of FI")

        squished_columns = ["businessname", "licenseno",
                            "address", "city", "state", "zip", "location"]

        DF_S = DF[squished_columns].set_index("licenseno")
        DF_S = DF_S.loc[~DF_S.index.duplicated()]
        DF_S["_id"] = DF_S.index.values

        r_dict = DF_S.to_dict(orient="record")

        repo.dropCollection("food_inspections_squished")
        repo.createCollection("food_inspections_squished")

        repo['kzhang21_ryuc_zui_sarms.food_inspections_squished'].insert_many(
            r_dict)

        log.debug("Finished with the squished version of FI")
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')
        # The scripts are in <folder>#<filename> format.
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        # The event log.
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:kzhang21_ryuc_zui_sarms#FoodInspection',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:4582bec6-2b4f-4f9e-bc55-cbaa73117f4c',
                              {'prov:label': 'Food Establishment Inspections', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        get_fi = doc.activity(
            'log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_fi, this_script)
        doc.usage(get_fi, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:DataSet': '03693648-2c62-4a2c-a4ec-48de2ee14e18/resource/{RES_ID}/download/tmp1yzpct9p.csv'
                   }
                  )
        fi = doc.entity('dat:kzhang21_ryuc_zui_sarms#FoodInspection',
                        {prov.model.PROV_LABEL: 'Food Inspections', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fi, this_script)
        doc.wasGeneratedBy(fi, get_fi, endTime)
        doc.wasDerivedFrom(fi, resource, get_fi, get_fi, get_fi)

        repo.logout()

        return doc

# eof