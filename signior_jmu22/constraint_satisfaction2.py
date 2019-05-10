import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import urllib.request
import pandas as pd
import math

class constraint_satisfaction2(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = ['signior_jmu22.car_data', 'signior_jmu22.population', 'signior_jmu22.countries_change_in_carbon_after_year', 'signior_jmu22.carbon_emissions']
  writes = ['signior_jmu22.constraint_satisfaction2']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')

    population = list(repo.signior_jmu22.population.find({}, {'_id': 0}))
    car_data = list(repo.signior_jmu22.car_data.find({}, {'_id': 0}))
    constraint_satisfaction = list(repo.signior_jmu22.countries_change_in_carbon_after_year.find())
    carbon_emissions = list(repo.signior_jmu22.carbon_emissions.find({}, {'_id': 0}))



    df_population = pd.DataFrame(population)
    df_constraint_satisfaction = pd.DataFrame(constraint_satisfaction)
    df_car_data = pd.DataFrame(car_data)
    df_carbon_emissions = pd.DataFrame(carbon_emissions)

    for row in carbon_emissions:
      if row.get('Country Name') == 'Korea, Dem. People’s Rep.':
        row['Country Name'] = 'North Korea'
      elif row.get('Country Name') == 'Korea, Rep.':
        row['Country Name'] = 'South Korea'
      elif row.get('Country Name') == 'United States':
        row['Country Name'] = 'United States of America'
    for row in population:
      if row.get('Country Name') == 'Korea, Dem. People’s Rep.':
        row['Country Name'] = 'North Korea'
      elif row.get('Country Name') == 'Korea, Rep.':
        row['Country Name'] = 'South Korea'
      elif row.get('Country Name') == 'United States':
        row['Country Name'] = 'United States of America'
    for row in car_data:
      if row.get('Country Name') == 'United States':
        row['Country Name'] = 'United States of America'
    
    population_dict = df_population.to_dict(orient='records')
    carbon_emissions_dict = df_carbon_emissions.to_dict(orient='records')
    car_data_dict = df_car_data.to_dict(orient='records')

    constraint_satisfaction_dict = df_constraint_satisfaction.to_dict(orient='records')

    # get countries in original constraint_satisfaction
    # get car data with same countries
    countries = [row.get('country') for row in constraint_satisfaction_dict]
    car_data_countries = [row for row in car_data if row.get('Country Name') in countries]
    carbon_emissions = [row for row in carbon_emissions if row.get('Country Name') in countries]

    car_data_world = [row for row in car_data_dict if row.get('Country Name') == 'World'][0]
    carbon_emissions_world = [row for row in carbon_emissions_dict if row.get('Country Name') == 'World'][0]
    del car_data_world['2003']
    del car_data_world['2010']

    car_data_countries.append(car_data_world)
    carbon_emissions.append(carbon_emissions_world)
    countries.append("World")
    # modify car data to reflect population, cars per 1000 people to cars per country
    for row in car_data_countries:
      country = row.get('Country Name')
      population_row = [pop_row for pop_row in population_dict if pop_row.get("Country Name") == country]
      if len(population_row) == 0:
        continue
      population_row = population_row[0]
      for key in row:
        if (key != 'Country Name'):
          # get the keys year
          num_thousand = int(population_row.get(key)) / 1000
          row[key] = round(row.get(key) * num_thousand)
    
    # modify carbon emissions to reflect population, metric tons per capita to metric tons general
    for row in carbon_emissions:
      country = row.get('Country Name')
      population_row = [pop_row for pop_row in population_dict if pop_row.get("Country Name") == country]
      if (len(population_row) == 0):
        continue
      population_row = population_row[0]
      for key in row:
        if (key != 'Country Name'):
          if (math.isnan(population_row.get(key))):
            continue
          else:
            row[key] = round(row.get(key) * population_row.get(key), 3)
    
    resulting_data = []
    for country in countries:
      new_row = {}
      new_row['country'] = country
      new_row['car_data'] = []
      new_row['carbon_emissions'] = []

      car_data_row = [data_row for data_row in car_data_countries if data_row.get('Country Name') == country]
      carbon_emissions_row = [data_row for data_row in carbon_emissions if data_row.get('Country Name') == country]

      if (len(car_data_row) == 0 or len(carbon_emissions_row) == 0):
        continue
      else:
        car_data_row = car_data_row[0]
        carbon_emissions_row = carbon_emissions_row[0]
      
      years = []
      for key in car_data_row:
        if (key == 'Country Name'):
          continue
        temp = {}
        years.append(key)
        temp[key] = car_data_row[key]
        new_row['car_data'].append(temp)
      
      for key in carbon_emissions_row:
        if (key == 'Country Name' or key not in years):
          continue
        temp = {}
        temp[key] = carbon_emissions_row[key]
        new_row['carbon_emissions'].append(temp)

      
      if (len(new_row['car_data']) == 0 or len(new_row['carbon_emissions']) == 0):
        continue
      else:
        curr_row_car_data = new_row['car_data']
        curr_row_emissions_data = new_row['carbon_emissions']

        new_row['percentage_change'] = []

        # find change in # cars and carbon emissions
        for i in range(len(curr_row_car_data) - 1):
          change_data = {}
          temp_row = curr_row_car_data[i]
          temp_row_key = list(temp_row.keys())[0]
          temp_row_value = temp_row[temp_row_key]

          next_row = curr_row_car_data[i+1]
          next_row_key = list(next_row.keys())[0]
          next_row_value = next_row[next_row_key]

          carbon_row = curr_row_emissions_data[i]
          carbon_row_key = list(carbon_row.keys())[0]
          carbon_row_value = carbon_row[carbon_row_key]

          next_carbon_row = curr_row_emissions_data[i+1]
          next_carbon_row_key = list(next_carbon_row.keys())[0]
          next_carbon_row_value = next_carbon_row[next_carbon_row_key]
          
          percentage_change_cars = (next_row_value - temp_row_value) / temp_row_value
          percentage_change_co2 = (next_carbon_row_value - carbon_row_value) / carbon_row_value

          
          # print(temp_row_key, percentage_change)
          change_data[temp_row_key] = (percentage_change_cars, percentage_change_co2)
          new_row['percentage_change'].append(change_data)
        
        percentage_change = new_row['percentage_change']
        
        if (len(percentage_change) == 0):
          continue
        co2_avg = 0
        car_avg = 0
        count = 0
        for row in percentage_change:
          key = list(row.keys())[0]
          (car_change, co2_change) = row.get(key)
          co2_avg += co2_change
          car_avg += car_change
          count += 1
        co2_avg = co2_avg / count
        car_avg = car_avg / count
        new_row['car_change_avg'] = car_avg
        new_row['co2_change_avg'] = co2_avg

        resulting_data.append(new_row)

    with open('test.txt', 'w') as f:
      print(resulting_data, file=f)  # Python 3.x
    repo.dropCollection("constraint_satisfaction2")
    repo.createCollection("constraint_satisfaction2")
    repo['signior_jmu22.constraint_satisfaction2'].insert_many(resulting_data)
    repo['signior_jmu22.constraint_satisfaction2'].metadata({'complete': True})

    print(repo['signior_jmu22.constraint_satisfaction2'].metadata())

    repo.logout()

    endTime = datetime.datetime.now()

    return {"start": startTime, "end": endTime}
   
    

  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/signior_jmu22.zhangyb')
    doc.add_namespace('dat', 'http://datamechanics.io/data/signior_jmu22')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log/')

    this_script = doc.agent('alg:signior_jmu22#constraint_satisfaction2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    
    resource_carbon_emissions = doc.entity('dat:signior_jmu22#carbon_emissions', {prov.model.PROV_LABEL: 'Yearly carbon emissions data', prov.model.PROV_TYPE:'ont:DataSet'})
    get_carbon_emissions= doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime) 
    doc.wasAssociatedWith(get_carbon_emissions, this_script)
    doc.usage(get_carbon_emissions, resource_carbon_emissions, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    resource_population = doc.entity('dat:signior_jmu22#population', {prov.model.PROV_LABEL: 'population data', prov.model.PROV_TYPE:'ont:DataSet'})
    get_resource_population = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_resource_population, this_script)
    doc.usage(get_resource_population, resource_population, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    
    resource_car_data = doc.entity('dat:signior_jmu22#car_data', {prov.model.PROV_LABEL: 'Yearly # Cars data', prov.model.PROV_TYPE:'ont:DataSet'})
    get_car_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_car_data, this_script)
    doc.usage(get_car_data, resource_car_data, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    resource_constraint_satisfaction = doc.entity('dat:signior_jmu22#countries_change_in_carbon_after_year', {prov.model.PROV_LABEL: 'Change in carbon after powerplant', prov.model.PROV_TYPE: 'ont:DataSet'})
    get_resource_constraint_satisfaction = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_resource_constraint_satisfaction, this_script)
    doc.usage(get_resource_constraint_satisfaction, resource_constraint_satisfaction, startTime, None, {prov.model.PROV_TYPE:'ont:Retreival'})

    get_constraint_satisfaction2 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    constraint_satisfaction2 = doc.entity('dat:signior_jmu22#constraint_satisfaction2', {prov.model.PROV_LABEL: 'Change in carbon emissions and change in cars per year per country', prov.model.PROV_TYPE: 'ont:DataSet'})

    doc.wasAttributedTo(constraint_satisfaction2, this_script)
    doc.wasGeneratedBy(constraint_satisfaction2, get_constraint_satisfaction2, endTime)
    doc.wasDerivedFrom(constraint_satisfaction2, resource_carbon_emissions, get_carbon_emissions, get_carbon_emissions, get_carbon_emissions)
    doc.wasDerivedFrom(constraint_satisfaction2, resource_population, get_resource_population, get_resource_population, get_resource_population)
    doc.wasDerivedFrom(constraint_satisfaction2, resource_car_data, get_car_data, get_car_data, get_car_data)
    doc.wasDerivedFrom(constraint_satisfaction2, resource_constraint_satisfaction, get_resource_constraint_satisfaction, get_resource_constraint_satisfaction, get_resource_constraint_satisfaction)


    return doc


# constraint_satisfaction2.execute()
# doc = constraint_satisfaction2.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))