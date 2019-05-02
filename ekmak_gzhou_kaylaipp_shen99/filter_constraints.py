import urllib.request
import json
import dml
import prov.model
from datetime import datetime
import uuid
import zillow 
import requests
import xmltodict
import csv
import time
import pandas as pd
import copy 
import random 
from tqdm import tqdm

class filter_constraints(dml.Algorithm):
    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = ['ekmak_gzhou_kaylaipp_shen99.accessing_data', 'ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data']
    writes = ['ekmak_gzhou_kaylaipp_shen99.constraints_filter']

    '''
    ------------------------------------------------------------
    Return dataset that fits the system constraints specified by user

    PARAMTERS: 
    - budget (AV_BLDG)
    - type of property (LU)
        * A = Residential 7 or more units
        * CD = Residential condimunium UNIT
        * CC = Commericial condimunium 
        * CM = Condimunium main (main buidling with all units, no accessed value though)
        * R1 = Residential 1 family 
        * R2 = Residential 2 family 
        * R3 = Residential 3 family 
        * R4 = Residential 4 family or more 
    - number of bedrooms (R_BDRMS)
    - number of rooms (R_TOTAL_RMS)
    - residential overall condition (R_OVRALL_CND)
        * A = average
        * G = Good
        * E = Excellent
        * P = Poor
        * F = Fair
        * S = Special 
    -------------------------------------------------------------
    '''
    @staticmethod
    def constraint_satisfaction(accessing_data,zillow_data, budget, type_of_property, number_bedrooms, number_rooms, condition):
        
        res = []
        # print('before: ', len(accessing_data))

        # if user specified a param, filter the results 
        if budget: 
            for idx,info in enumerate(accessing_data): 
                valuation = info['AV_BLDG']
                if valuation <= budget: 
                    res.append(info)

        # print('num properties that fit budget: ', len(res))


        if type_of_property: 
            property_filter = []
            for idx,info in enumerate(res): 
                type_ = info['LU']
                # print(type_, type_of_property)
                if str(type_) == str(type_of_property): 
                    property_filter.append(info)    

            res = property_filter
        # print('num properties that fit type: ', len(res))    


        if number_bedrooms: 
            bedroom_filter = []
            for idx,info in enumerate(res): 
                beds = info['R_BDRMS']

                # if match number bedrooms, add to new list
                if beds == str(number_bedrooms): 
                    bedroom_filter.append(info)
            res = bedroom_filter

        # print('num properties that fit bedrooms: ', len(res))    

        if number_rooms: 
            room_filter = []
            for idx,info in enumerate(res): 
                rooms = info['R_TOTAL_RMS']
                # print(rooms, number_rooms)
                # if over budget, delete from total list 
                if rooms == str(number_rooms): 
                    room_filter.append(info)    
            res = room_filter          

        # print('num properties that fit rooms: ', len(res))     

        if condition: 
            condition_filter = []
            for idx,info in enumerate(tqdm(res)): 
                cond = info['R_OVRALL_CND']

                # if over budget, delete from total list 
                if cond == condition: 
                    condition_filter.append(info)  
            res = condition_filter            

        # print('num properties that fit condition: ', len(res))    
        return res

    # parse zillow search dataset to find price to address
    @staticmethod
    def get_price(address, zillow_data): 
        price = 0
        for info in zillow_data: 
            zillow_addr = info['full_address']['street'].lower()

            if zillow_addr == address: 
                price = info['zestimate']['amount']
        return price 
    
    '''
    ---------------------------------------------------
    Given a dataset that fits a set of a constraints 
    return the optimal combination of properties to buy 

    optimize by: 
        - condition (sort by best condition?? return combo of properties where sum price fits budget)
        - high_price (sort by high price to lowest, return combo of properties where sum price fits budget)
        - low_price (sort by low price to highest, return combo of properties where sum price fits budget)
    ---------------------------------------------------
    '''
    @staticmethod
    def optimize(dataset, zillow_data, optimize_by, budget): 
        results = []

        # test, convert to dictionary, see all prices 
        test = pd.DataFrame(dataset)

        if optimize_by == 'high_price': 

            # sort by price high to low 
            sorted_price = sorted(dataset, key=lambda k: k['AV_BLDG'], reverse = True) 

            # go through sorted price 
            for idx, info in enumerate(sorted_price): 

                # if budget not negative, see if we can buy more houses 
                if budget >= 0: 

                    price = info['AV_BLDG']
                    st_name = info['ST_NAME'].lower().replace(".", "")         #beacon
                    st_num = info['ST_NUM'].replace(" ", "")                    #362
                    st_suffix = info['ST_NAME_SUF'].lower().replace(".", "")   #st
                    full_address = st_num + ' '+ st_name + ' ' + st_suffix

                    if price is None or price == 0: 

                        # try to get price from zillow data 
                        price = get_price(full_address, zillow_data)
                        info['AV_BLDG'] = price

                    # if within budget, buy 
                    if price <= budget and price != 0: 
                        # buy house, update budget, add property to result 
                        budget -= price 
                        info['optimized_type'] = 'high_price'
                        results.append(info)

                    # other wise dont buy, continue searching
                    else: 
                        continue 

            # print('budget: ', budget)
            # print('results high price: ', len(results))
            return results

        if optimize_by == 'low_price':

            # sort by price high to low 
            sorted_price = sorted(dataset, key=lambda k: k['AV_BLDG'], reverse = False) 

            # go through sorted price 
            for idx, info in enumerate(sorted_price): 

                # if budget not negative, see if we can buy more houses 
                if budget > 0: 

                    price = info['AV_BLDG']

                    # if within budget, buy 
                    if price <= budget and price != 0: 
                        # buy house, update budget, add property to result 
                        budget -= price 
                        info['optimized_type'] = 'low_price'
                        results.append(info)

                    # other wise dont buy, continue searching
                    else: 
                        continue 

            # print('budget: ', budget)
            # print('results low price: ', len(results))
            return results
    
    @staticmethod
    def execute(trial):
        startTime = datetime.now()

        #connect to database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')
        print('')
        print('inserting filter constraints data...')

        # read in accessing data 
        accessing_data = repo.ekmak_gzhou_kaylaipp_shen99.accessing_data.find()

        # read in zillow property data (for prices if not listed in accessing data)
        zillow_data = repo.ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data.find()

        # if in trial mode, take random section of dataset 
        if trial: 
            print('in trial mode!')
            ranges = random.sample(range(0, accessing_data.count()), 2)
            start = min(ranges)
            end = max(ranges)
            accessing_data = list(accessing_data)[start:end]

        # convert to pandas dataframe 
        accessing_df = pd.DataFrame(list(accessing_data))

        # change price valuations to int so we can compare prices 
        accesing_df = accessing_df.astype({"AV_BLDG": int})

        # convert dataframe back to list of dictionaries 
        accessing_data = list(accesing_df.T.to_dict().values())


        # arbitrary paramters to filter by 
        # when building site, user would be able to pick these 
        budget = 1000000
        type_of_property = 'R1'
        number_bedrooms = None
        number_rooms = None
        condition = 'G'

        this = filter_constraints

        # filter constraints 
        filtered = this.constraint_satisfaction(accessing_data, zillow_data, budget, type_of_property, number_bedrooms, number_rooms, condition)

        # optimize budget 
        optimized_low_price = this.optimize(filtered, zillow_data, 'low_price', budget)
        optimized_high_price = this.optimize(filtered, zillow_data, 'high_price', budget)

        # clear database 
        repo.dropCollection("constraints_filter")
        repo.createCollection("constraints_filter")

        # store information in db 
        repo['ekmak_gzhou_kaylaipp_shen99.constraints_filter'].insert_many(optimized_low_price)
        repo['ekmak_gzhou_kaylaipp_shen99.constraints_filter'].insert_many(optimized_high_price)

        # print('stored constraints in db')

        # Store information in db
        repo.logout()
        endTime = datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
# Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_agent = doc.agent('alg:ekmak_gzhou_kaylaipp_shen99#constraints_filter',{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        this_entity = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#constraints_filter',
                            {prov.model.PROV_LABEL: 'Property results given set of constraints', prov.model.PROV_TYPE: 'ont:DataSet'})


        accessing_resource = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#accessing_data',
		                  {prov.model.PROV_LABEL: 'Accessing Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        zillow_resource = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#zillow_getsearchresults_data',
		                  {prov.model.PROV_LABEL: 'Zillow Search Results 2', prov.model.PROV_TYPE: 'ont:DataSet'})
        

        get_properties_from_filters = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(get_properties_from_filters, zillow_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_properties_from_filters, accessing_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(get_properties_from_filters, this_agent)

        doc.wasAttributedTo(this_entity, this_agent)

        doc.wasGeneratedBy(this_entity, get_properties_from_filters, endTime)

        doc.wasDerivedFrom(this_entity, zillow_resource, accessing_resource, get_properties_from_filters, get_properties_from_filters, get_properties_from_filters)

        repo.logout()
                  
        return doc
        

# if __name__ == "__main__":
#     filter_constraints.execute()
# filter_constraints.execute(True)
# filter_constraints.provenance()
# print('prov done!')



    


