import dml
import prov.model
import datetime
import uuid
import pandas as pd 
import numpy as np
import math
import json
import random

from scipy import stats
from collections import OrderedDict

def linreg(data_x, data_y):
  data_x = np.array(data_x)
  data_y = np.array(data_y)
  return stats.linregress(data_x, data_y)
def pearsonr(data_x, data_y):
  data_x = np.array(data_x)
  data_y = np.array(data_y)
  return stats.pearsonr(data_x, data_y)

class statistical_analysis(dml.Algorithm):
  contributor = "signior_jmu22"
  reads = ["signior_jmu22.constraint_satisfaction2"]
  writes = ["signior_jmu22.total_statisical_analysis"]

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')

    constraint_satisfaction2 = list(repo.signior_jmu22.constraint_satisfaction2.find({}, {'_id': 0}))
    statistics = []
    data_x_total = []
    data_y_total = []
    for row in constraint_satisfaction2:
      stats_row = {}
      stats_row['country'] = row['country']
      
      car_data = row['car_data']
      co2_data = row['carbon_emissions']
      if len(car_data) < 5:
        continue
      data_x = []
      data_y = []
      for car_row in car_data:
        year_key = list(car_row.keys())[0]
        entry = car_row[year_key]
        data_x.append(entry)
      for co2_row in co2_data:
        year_key = list(co2_row.keys())[0]
        entry = co2_row[year_key]
        data_y.append(entry)
      (slope, intercept, r_value, p_value, std_err) = linreg(data_x, data_y)
      stats_row['slope'] = slope
      stats_row['intercept'] = intercept
      stats_row['r_value'] = r_value
      stats_row['r_squared'] = r_value ** 2
      stats_row['p_value'] = p_value
      statistics.append(stats_row)
    
    global_data_car = {}
    global_data_emissions = {}
    for row in constraint_satisfaction2:
      car_data = row['car_data']
      co2_data = row['carbon_emissions']
      for car_row in car_data:
        year_key = list(car_row.keys())[0]
        entry = car_row[year_key]
        global_data_car[year_key] = global_data_car.get(year_key, 0) + entry
      for co2_row in co2_data:
        year_key = list(co2_row.keys())[0]
        entry = co2_row[year_key]
        global_data_emissions[year_key] = global_data_emissions.get(year_key, 0) + entry
    
    global_data_car = OrderedDict(sorted(global_data_car.items()))
    global_data_emissions = OrderedDict(sorted(global_data_emissions.items()))
    global_car = []
    global_emissions = []
    for year in range(2003, 2011):
      year = str(year)
      global_car.append(global_data_car[year])
      global_emissions.append(global_data_emissions[year])
    
    global_stats = {}
    global_stats['country'] = 'World'
    (slope, intercept, r_value, p_value, std_err) = linreg(global_car, global_emissions)
    global_stats['slope'] = slope
    global_stats['intercept'] = intercept
    global_stats['r_value'] = r_value
    global_stats['r_squared'] = r_value ** 2
    global_stats['p_value'] = p_value

    statistics.append(global_stats)

    # print("Model doesn't fit")
    # print("---------")
    # for row in statistics:
    #   if (row.get('p_value') > 0.05):
    #     print(row.get('country'))
    # print(" ")
    # print("Model is statistically significant")
    # print("---------")
    # for row in statistics:
    #   if (row.get('p_value') < 0.05):
    #     print(row.get('country'))
    
    repo.dropCollection("total_statistical_analysis")
    repo.createCollection("total_statistical_analysis")
    repo['signior_jmu22.total_statistical_analysis'].insert_many(statistics)
    repo['signior_jmu22.total_statistical_analysis'].metadata({'complete': True})

    print(repo['signior_jmu22.total_statistical_analysis'].metadata())

    repo.logout()

    endTime = datetime.datetime.now()

    return {"start": startTime, "end": endTime}


  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/signior_jmu22') # The scripts are in <folder>#<filename> format
    doc.add_namespace('dat', 'http://datamechanics.io/data/signior_jmu22' ) # The datasets are in <user>#<collection> format
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retreival', 'Query', or 'Computation'
    doc.add_namespace('log', 'http://datamechanics.io/log/')

    this_script = doc.agent('alg:signior_jmu22#statistical_analysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    constraint_satisfaction2 = doc.entity('dat:signior_jmu22#constraint_satisfaction2', {prov.model.PROV_LABEL:'Car and CO2 emissions accumulation dataset', prov.model.PROV_TYPE:'ont:DataSet'})

    get_statistical_analysis = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_statistical_analysis, this_script)

    doc.usage(get_statistical_analysis, constraint_satisfaction2, startTime, None, {prov.model.PROV_TYPE:'ont:Computation'})

    statistical_analysis = doc.entity('dat:signior_jmu22#statistical_analysis_data', {prov.model.PROV_LABEL: 'r squared value and p values for each country relationship between vehicles and co2', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(statistical_analysis, this_script)
    doc.wasGeneratedBy(statistical_analysis, get_statistical_analysis, endTime)
    doc.wasDerivedFrom(statistical_analysis, constraint_satisfaction2, get_statistical_analysis, get_statistical_analysis, get_statistical_analysis)

    repo.logout()

    return doc

# comment this out when submitting
# statistical_analysis.execute()
# doc = statistical_analysis.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))