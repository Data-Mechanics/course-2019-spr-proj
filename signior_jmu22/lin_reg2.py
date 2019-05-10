import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import urllib.request
import pandas as pd
import statistics
import numpy as np
from scipy import polyfit
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import random
from scipy import stats
import math
import collections


class lin_reg2(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = ['signior_jmu22.carbon_emissions', 'signior_jmu22.signior_jmu22.power_plants_established_date_by_country', 'signior_jmu22.countries_change_in_carbon_after_year', 'signior_jmu22.population']
  writes = ['signior_jmu22.lin_reg2']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    # Set up database connection
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')
    
 
    
    
    # gets list of countries that meet the constraint satisfaction
    
    temp2 = list(repo.signior_jmu22.countries_change_in_carbon_after_year.find())
    df2 = pd.DataFrame(temp2)
    results2 = df2.to_dict(orient = "records")
    
    countries = [rows.get('country') for rows in results2]
  
        
    
    
    
    
    temp = list(repo.signior_jmu22.carbon_emissions.find())
    df = pd.DataFrame(temp)
    results = df.to_dict(orient = "records")

       
    for row in results:
      if row.get('Country Name') == 'Korea, Dem. People’s Rep.':
        row['Country Name'] = 'North Korea'
      elif row.get('Country Name') == 'Korea, Rep.':
        row['Country Name'] = 'South Korea'
      elif row.get('Country Name') == 'United States':
        row['Country Name'] = 'United States of America'
   

    



    temp3 = list(repo.signior_jmu22.power_plants_established_date_by_country.find())
    df3 = pd.DataFrame(temp3)
    results3 = df3.to_dict(orient = "records")

    year_arr = []
    for pp in results3:
        for year in pp['years']:
            year_arr.append(year)
        
    lst = collections.Counter(year_arr)
    #print(sorted(lst.items()))  
    
    
    years = []
    for year in range(1960, 2015):
        count = 0 
        for x in sorted(lst.items()):
            count += x[1]
            if x[0] == year:
                years.append({year : count})
    #print(years)
    
   
   


        
            
                
            
        
    temp4 = list(repo.signior_jmu22.population.find())
    df4 = pd.DataFrame(temp4)
    results4= df4.to_dict(orient = "records")
    

         
    for row in results4:
      if row.get('Country Name') == 'Korea, Dem. People’s Rep.':
        row['Country Name'] = 'North Korea'
      elif row.get('Country Name') == 'Korea, Rep.':
        row['Country Name'] = 'South Korea'
      elif row.get('Country Name') == 'United States':
        row['Country Name'] = 'United States of America'
     

    for rows in results:
        if (rows.get('Country Name') == "World"):
            carbon = rows
            
    for rows in results4:
        if (rows.get('Country Name') == "World"):
            population = rows
    
    
    
    
    total = [{year: carbon[str(year)] * population[str(year)]} for year in range(1960, 2015)]
    
    world_carbon_arr = []
    

    for entry in total:
        world_carbon_arr.append(list((entry.values())))
        
        
    foo = []

    for entry in world_carbon_arr:
        for x in entry:
            foo.append(x)
            
    #world population carbon emissions array)
    #print(foo)

    wrld_yrs = []
    for entry in years:
        wrld_yrs.append(list((entry.values())))
    
    foo2 = []
    #print(wrld_yrs)
    
    for entry in wrld_yrs:
        for x in entry:
            
            foo2.append(x)
    #print(foo2)
    
    
    data_world = stats.linregress(np.array(foo2), np.array(foo))


    
    carbon_emissions= [rows for rows in results if rows.get('Country Name') in countries]
    power_plants =[rows for rows in results3 if rows.get('country') in countries]
    population_dict = [row for row in results4 if row.get('Country Name') in countries]
    

    
    total_carbon = []
    result = []
    
    for country in countries:
        curr_row = {}
        curr_row['country'] = country
        curr_row['data'] = []
        templist = {}
        templist['country'] = country
        templist['carbon'] = []
        templist['plants'] = []
        carbon_emissions_row = list(filter(lambda row: row['Country Name'] == country, carbon_emissions))[0]
        power_plant_data_row = list(filter(lambda row: row['country'] == country, power_plants))[0]
        population_data_row = list(filter(lambda row: row['Country Name'] == country, population_dict))[0]


        country_plants = []
        country_carbon= []
        worldplantscount = 0
        wordcarboncount = 0 
        
        
        
        
        power_plant_years = sorted(power_plant_data_row['years'])
        for curr_year in power_plant_years:
            if (curr_year > 2014):
              break
            if (carbon_emissions_row.get(str(curr_year)) is None or population_data_row.get(str(curr_year)) is None or carbon_emissions_row.get(str(curr_year - 1)) is None or population_data_row.get(str(curr_year - 1)) is None or carbon_emissions_row.get(str(curr_year + 1)) is None or population_data_row.get(str(curr_year + 1)) is None):
              continue
            year_curr = carbon_emissions_row.get(str(curr_year)) * population_data_row.get(str(curr_year))
            
            if (math.isnan(year_curr)):
                continue
            else:

                country_plants.append(len(curr_row['data']))
                country_carbon.append(year_curr)
           
                
            
     
            
            curr_row['data'].append({str(len(curr_row['data'])): year_curr})
           
        total_carbon.append(curr_row)
        
        templist['carbon'].append(country_carbon)
        templist['plants'].append(country_plants)
       
        result.append(templist)
        


    #print(result)
    stats_array = []
    for country in result:
        
        if len(country['plants'][0])< 2 or len(country['carbon'][0])  < 2 :
            continue

        else:
            
            
            data = stats.linregress(np.array(country['plants'][0]), np.array(country['carbon'][0]))
        
            stats_array.append({country['country'] : data})
            
            
        
    stats_array.append({'World' : data_world})
     
    print(stats_array)
   
        
            
 #plots linear regression in an external file 
                    
    # with PdfPages('linreg_countries.pdf') as pdf:
    #     #item[1] == x item[2] = y
    #     for item in templist:
    #         corrcoeff = round(np.corrcoef(item[1], item[2])[1,0],2)
    #         pl = polyfit(item[1], item[2], 1)
    #         plt.figure()
    #         plt.figtext(0.1,0.9,"Country: " + str(item[0]) +" CorrCoeff: "+str(corrcoeff) + "Average Mean of Change: " + str(round(country.get(item[0]),2)))
    #         plt.plot(item[1], item[2], 'o')
    #         plt.plot(item[1],np.polyval(pl,item[1]), 'r--')
    #         pdf.savefig()
    
           
    #concatenate everything into (country, tempx, tempx, corcoeff, average mean of change)

    


    repo.dropCollection("lin_reg2")
    repo.createCollection("lin_reg2")
    repo['signior_jmu22.lin_reg2'].insert_many(stats_array)
    repo['signior_jmu22.lin_reg2'].metadata({'complete': True})
    print(repo['signior_jmu22.lin_reg2'].metadata())
    repo.logout()
    endTime = datetime.datetime.now()
    return {"start": startTime, "end": endTime}
      
#    
  
  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/signior_jmu22') # The scripts are in <folder>#<filename> format
    doc.add_namespace('dat', 'http://datamechanics.io/data/signior_jmu22' ) # The datasets are in <user>#<collection> format
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retreival', 'Query', or 'Computation'
    doc.add_namespace('log', 'http://datamechanics.io/log/')
    
    this_script = doc.agent('alg:signior_jmu22#lin_reg2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
                                    
    resource_carbon_emissions= doc.entity('dat:signior_jmu22#carbon_emissions', {'prov:label': 'Yearly carbon emissions data', prov.model.PROV_TYPE: 'ont:DataResource'})
    get_carbon_emissions = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_carbon_emissions, this_script)
    doc.usage(get_carbon_emissions, resource_carbon_emissions, startTime, None, {prov.model.PROV_TYPE:'ont:Retreival'})
    
    resource_power_plant = doc.entity('dat:signior_jmu22#power_plant', {prov.model.PROV_LABEL: 'carbon emissions data', prov.model.PROV_TYPE:'ont:DataSet'})
    get_power_plant = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_power_plant, this_script)
    doc.usage(get_power_plant, resource_power_plant, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    resource_population = doc.entity('dat:signior_jmu22#population', {prov.model.PROV_LABEL: 'population data', prov.model.PROV_TYPE:'ont:DataSet'})
    get_resource_population = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_resource_population, this_script)
    doc.usage(get_resource_population, resource_population, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    
    
    get_lin_reg2 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    reg_data = doc.entity('dat:signior_jmu22#lin_reg2', {prov.model.PROV_LABEL: 'Linear Regression Model for each country using scipy linregress', prov.model.PROV_TYPE: 'ont:DataSet'})

    doc.wasAttributedTo(reg_data, this_script)
    doc.wasGeneratedBy(reg_data, get_lin_reg2, endTime)
    doc.wasDerivedFrom(reg_data, resource_carbon_emissions, get_lin_reg2, get_lin_reg2, get_lin_reg2)
    doc.wasDerivedFrom(reg_data, resource_population, get_lin_reg2, get_lin_reg2, get_lin_reg2)
    doc.wasDerivedFrom(reg_data, resource_power_plant, get_lin_reg2, get_lin_reg2, get_lin_reg2)

    
    repo.logout()
    


    return doc



#comment this when submitting, just for testing purposes
# lin_reg2.execute()
# doc = linear_regression.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
  