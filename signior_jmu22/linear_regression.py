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


class linear_regression(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = ['signior_jmu22.countries_change_in_carbon_after_year']
  writes = ['signior_jmu22.linear_regression']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    # Set up database connection
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')
    
    temp = list(repo.signior_jmu22.countries_change_in_carbon_after_year.find())
    df = pd.DataFrame(temp)
    results = df.to_dict(orient = "records")
    #print(results)
    
    templist = [] 
    range_arr = list(range(1960, 2018))

    if (trial == True):
        results = random.sample(results, 10)

    # gets country and the array of carbon increases before stored in temp x and array of carbon increases after a power plant in y
    for rows in results:
        tempX = []
        tempY = []
        if len((rows.get('data'))) >= 3:
#            print(rows.get('data'))
            for entry in rows.get('data'):
                for i in range_arr:
                    if entry.get(str(i)) is None:
                        continue
                    else:
#                        print(entry.get(str(i)))
                        tempX.append(entry.get(str(i))[0])
                        tempY.append(entry.get(str(i))[1])
    
            templist.append((rows.get('country'), tempX, tempY))
    #print(templist)
    
    
    
    #gets the average mean of the difference between  the increase in the years before and after a powerplant was invented

    templist2 = []
    for x in templist:
        country = []
        country.append(x[0])
        difflist = []
        for i in range(0, len(x[1])):
            difflist.append((x[2][i] - x[1][i]))
        country.append(difflist)
        templist2.append(country)
    
    maxlist = 0 
    country = {}
    for y in templist:

        for i in range(0, len(y[1])):
            maxlist+=y[1][i]
        country[y[0]] = (maxlist / len(y[1]))
        maxlist = 0

        
            
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
    finallist = []
    
    for e in templist:
        finallist.append({'country': e[0], 'x_data':e[1], 'y_data': e[2], "corr_coef": round(np.corrcoef(e[1], e[2])[1,0],2), "mean": round(country.get(e[0]), 2)})
        
    print(finallist)
    
    
               

    repo.dropCollection("linear_regression")
    repo.createCollection("linear_regression")
    repo['signior_jmu22.linear_regression'].insert_many(finallist)
    repo['signior_jmu22.linear_regression'].metadata({'complete': True})
    print(repo['signior_jmu22.carbon_land_sea'].metadata())
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
    
    this_script = doc.agent('alg:signior_jmu22#linear_regression', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
    resource= doc.entity('dat:countries_change_in_carbon_after_year', {'prov:label': 'Countries change in carbon after year', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'csv'})
    get_lin_reg = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_lin_reg, this_script)
    doc.usage(get_lin_reg, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retreival'})
    
    lin_reg = doc.entity('dat:signior_jmu22#linear_regression', {prov.model.PROV_LABEL: 'Linear Regression', prov.model.PROV_TYPE: 'ont:DataSet'})
    doc.wasAttributedTo(lin_reg, this_script)
    doc.wasGeneratedBy(resource, endTime)
    doc.wasDerivedFrom(lin_reg, resource, get_lin_reg, get_lin_reg, get_lin_reg)
    
    repo.logout()
    


    return doc



#comment this when submitting, just for testing purposes
# linear_regression.execute()
# doc = linear_regression.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
  