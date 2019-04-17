import datetime
import logging
import uuid


import dml
import pandas as pd
import prov.model

# Is this URL permanent?
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class FoodViolations(dml.Algorithm):
    contributor = 'kzhang21_ryuc_zui_sarms'
    reads = ["kzhang21_ryuc_zui_sarms.food_inspections"]
    writes = ['kzhang21_ryuc_zui_sarms.food_violations']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        # This will fail to connect to the one require SSH auth
        client = dml.pymongo.MongoClient()
        repo = client.repo
        log.debug("Authenticating into mongoDB")
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')

        log.debug("Fetching data from kzhang21_ryuc_zui_sarms.food_inspections")
        df = pd.DataFrame(
            list(repo["kzhang21_ryuc_zui_sarms.food_inspections"].find())
        )

        log.debug("Project to select only the column we wants")
        selected_columns = ["businessname", "licenseno", "violstatus", "address", "city", "state", "zip", "property_id",
                            "location", "violdttm", "violation"]
        DF = df[selected_columns]

        # Select all the Fail violations
        DF = DF[DF["violstatus"] == "Fail"]

        DF["violdttmClean"] = DF["violdttm"].map(
            lambda x: x.strip()).map(lambda x: x if x else "NO_DATE")

        log.debug("Counting violations per restaurant")
        violation_DF = pd.DataFrame()

        violation_DF["violationCount"] = DF.groupby(
            "licenseno").count()["violation"]

        log.debug("Get the earliest violation date of each licenseno. (Index is licenseno).")
        violation_DF["violationDate"] = DF[DF["violdttmClean"] !=
                                           "NO_DATE"].groupby("licenseno").min()["violdttmClean"]
        violation_DF = violation_DF.dropna()

        new_columns_list = list(set(
            selected_columns) - {"violation", "violdttm", "licenseno", "violdttmClean"})

        RDF = DF.set_index("licenseno")[new_columns_list]

        log.debug("Removing duplicated")
        RDF = RDF.loc[~RDF.index.duplicated()]

        RDF["violationCount"] = violation_DF["violationCount"]
        RDF["violationDate"] = violation_DF["violationDate"]
        RDF["violationDateParsed"] = pd.to_datetime(RDF["violationDate"])
        RDF["violationDays"] = RDF["violationDateParsed"].map(
            lambda x: (startTime - x).days)
        RDF["violationRate"] = RDF["violationCount"]/RDF["violationDays"]
        RDF = RDF.loc[RDF["violationDays"].dropna().index, :]

        RDF["_id"] = RDF.index.values
        # DF_R["location"] = DF_R["location"].map(parse_coor)

        r_dict = RDF.to_dict(orient="record")

        repo.dropCollection("food_violations")
        repo.createCollection("food_violations")

        log.debug("🐼 PEACE!! Pushing data into mongoDB 🍃")
        repo['kzhang21_ryuc_zui_sarms.food_violations'].insert_many(r_dict)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # The scripts are in <folder>#<filename> format.
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        # The event log.
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#FoodViolations',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:zui_sarms#FoodInspection',
                              {prov.model.PROV_LABEL: 'Food Inspections', prov.model.PROV_TYPE: 'ont:DataSet'})
        get_fv = doc.activity(
            'log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_fv, this_script)
        doc.usage(get_fv, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'}
                  )
        fv = doc.entity('dat:kzhang21_ryuc_zui_sarms#FoodViolations',
                        {prov.model.PROV_LABEL: 'Food Establishment Violations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fv, this_script)
        doc.wasGeneratedBy(fv, get_fv, endTime)
        doc.wasDerivedFrom(fv, resource, get_fv, get_fv, get_fv)

        return doc


def parse_coor(s):
    """
    Parse the string to tuple of coordinate
    In the format of (lat, long)
    """

    lat, long = s.split(", ")
    lat = lat[1:]
    long = long[:-1]
    lat = float(lat)
    long = float(long)

    return [lat, long]
