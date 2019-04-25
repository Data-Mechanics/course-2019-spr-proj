import datetime
import uuid
from statistics import mean, stdev
import dml
import prov.model
from scipy.stats import pearsonr


class getStatistics(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = [contributor + ".Neighborhoods", contributor + ".ParcelsCombined"]
    writes = [contributor + ".Statistics"]

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(getStatistics.contributor, getStatistics.contributor)

        parcels = repo[getStatistics.contributor + ".ParcelsCombined"]
        neighborhoods = list(repo[getStatistics.contributor + ".Neighborhoods"].find())

        repo.dropCollection(getStatistics.contributor + ".Statistics")
        repo.createCollection(getStatistics.contributor + ".Statistics")
        for i in range(len(neighborhoods)):
            name = neighborhoods[i]["properties"]["Name"]
            data = list(parcels.find({"Neighborhood": name}))
            if len(data) > 0:
                # these are the three health statistics
                for category in ["obesity", "asthma", "low_phys"]:
                    x = []
                    y = []
                    for i in range(len(data)):
                        y.append(float(data[i][category]))
                        x.append(float(data[i]["distance_score"]))
                    # find r values and their p-value for the correlation
                    # between distance scores and a health catagory
                    corr = pearsonr(x, y)
                    repo[getStatistics.contributor + ".Statistics"].insert_one(
                        {"Neighborhood": name, "variable": category,
                         "statistic": "pearsonr", "value": corr})
                    m = mean(y)
                    repo[getStatistics.contributor + ".Statistics"].insert_one(
                        {"Neighborhood": name, "variable": category,
                         "statistic": "mean", "value": m})
                    repo[getStatistics.contributor + ".Statistics"].insert_one(
                        {"Neighborhood": name, "variable": category,
                         "statistic": "std_dev", "val": stdev(y, m)})

        # compute mean and std_dev of distance scores
        for i in range(len(neighborhoods)):
            name = neighborhoods[i]["properties"]["Name"]
            data = list(parcels.find({"Neighborhood": name}))
            if len(data) > 0:
                x = []
                for i in range(len(data)):
                    x.append(float(data[i]["distance_score"]))
                m = mean(x)
                repo[getStatistics.contributor + ".Statistics"].insert_one(
                    {"Neighborhood": name, "variable": "distance_score",
                     "statistic": "mean", "value": m})
                repo[getStatistics.contributor + ".Statistics"].insert_one(
                    {"Neighborhood": name, "variable": "distance_score",
                     "statistic": "std_dev", "value": stdev(x, m)})

        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        this_script = doc.agent('alg:gasparde_ljmcgann_tlux#getStatistics',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'],
                                 'ont:Extension': 'py'})

        Neighborhoods = doc.entity('dat:gasparde_ljmcgann_tlux#Neighborhoods',
                                   {prov.model.PROV_LABEL: 'Shape of Boston Neighborhoods',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        ParcelsCombined = doc.entity('dat:gasparde_ljmcgann_tlux#ParcelCombined',
                                     {prov.model.PROV_LABEL: 'Final Dataset Produced for Optimization and Analysis',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})

        getStatistics = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(getStatistics, this_script)
        doc.usage(getStatistics, Neighborhoods, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(getStatistics, ParcelsCombined, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        Stats = doc.entity('dat:gasparde_ljmcgann_tlux#Statistics',
                           {prov.model.PROV_LABEL: 'Various Statistics on Health and Open Space Data',
                            prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(Stats, this_script)

        doc.wasGeneratedBy(Stats, getStatistics, endTime)

        doc.wasDerivedFrom(Stats, Neighborhoods, getStatistics, getStatistics,
                           getStatistics)
        doc.wasDerivedFrom(Stats, ParcelsCombined, getStatistics, getStatistics,
                           getStatistics)
        return doc
