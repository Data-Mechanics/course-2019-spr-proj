from flask import Flask
from flask import render_template
from flask import request, redirect, flash
import re
import pymongo
import folium

from app import app

@app.route('/')
def index():
    y = request.form.get('input')
    print(y)
    return render_template('index.html')


# @app.route('/recievedata', methods=['GET','POST'])
@app.route('/', methods=['GET','POST'])
def recieve_data():
    min_price = request.form.get('min_price')
    max_price = request.form.get('max_price')
    bedroom = request.form.get('bedrooms')
    bathroom = request.form.get('bathrooms')
    cat = request.form.get('categories')

    if max_price != 'None': 
        max_price = max_price.replace("$", "")
        max_price = int(max_price.replace(",", ""))

    # filter properties 
    results = filter(max_price, bedroom, bathroom, cat)

    # create the map 
    create_map(results)

    print('map created! ')
    print('min price in recieeve data: ', min_price)
    print('max price in recieeve data: ', max_price)
    return show_data(min_price, max_price, bedroom, bathroom, cat, results)


'''
show filtered results 
'''
@app.route('/<min_price>/<max_price>', methods=['GET','POST'])
def show_data(min_price, max_price, bedroom, bathroom, type_, results):
    print("min price: ", min_price)
    print('max price: ', max_price)
    if request.method == 'POST':
        print('POST METHOD')
    else: 
        print('GET METHOD')
    return render_template('index.html', filters = True, min_price = min_price, 
                                        max_price = max_price, bedroom = bedroom, bathroom = bathroom, property_type = type_,
                                        results = results)

'''
-----------------------------------------
helper function that takes in filters 
and returns matching properties 

PARAMETERS: 
- bedroom
- bathroom
- property 

RETURNS: 
- list of properties 
-----------------------------------------
'''
def filter(budget, number_bedrooms, number_bathrooms, type_of_property):

        type_mapping = {'A': 'Residential 7 or more units', 'CD': 'Residential condimunium unit', 
                    'CC': 'Commercial condimunium', 'CM': 'Condomunium main', 'R1':'Residential 1 family', 
                    'R2': 'Residential 2 family', 'R3':'Residential 3 family','R4': 'Residential 4 family or more',
                    'None': 'N/A', 'E': 'Tax Exempt', 'I': 'Industrial', 'C':'Commercial', 'RC': 'Mixed Use', 'AH': 'Agricultural', 
                    'RL': 'Residential Land', 'EA': 'Tax Exempt (121A)', 'CL': 'Commercial Land', 'CP': 'Condo Parking'}

        # connect to database 
        client = pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')

        # get properties from db
        accessing_data = repo.ekmak_gzhou_kaylaipp_shen99.accessing_data.find()

        res = []
        # print('before: ', len(accessing_data))
        if budget != 'None':
            budget = int(budget)


        # if user specified a param, filter the results 
        if budget != 'None': 
            print('in budget')
            for idx,info in enumerate(accessing_data): 
                valuation = int(info['AV_BLDG'])
                if valuation <= int(budget): 
                    res.append(info)

        # if budget isn't specified, add all to results 
        else: 
            res = list(accessing_data)

        print('num properties that fit budget: ', len(res))

        # if type of property specified, filter through res list
        if type_of_property != 'All': 

            property_filter = []
            for idx,info in enumerate(res): 
                type_ = info['LU']
                type_ = type_mapping[type_]
                print(type_, type_of_property)
                if str(type_) == str(type_of_property): 
                    property_filter.append(info)    

            res = property_filter

        print('num properties that fit type: ', len(res))    


        if number_bedrooms != 'All': 
            bedroom_filter = []
            for idx,info in enumerate(res): 
                beds = info['R_BDRMS']

                # if match number bedrooms, add to new list
                if beds == str(number_bedrooms): 
                    bedroom_filter.append(info)
            res = bedroom_filter

        print('num properties that fit bedrooms: ', len(res))    

        if number_bathrooms != 'All': 
            room_filter = []
            for idx,info in enumerate(res): 
                rooms = info['R_FULL_BTH']
                # print(rooms, number_bathrooms)
                # if over budget, delete from total list 
                if rooms == str(number_bathrooms): 
                    room_filter.append(info)    
            res = room_filter          

        print('num properties that fit rooms: ', len(res))     
        return res

'''
create map based on filtered properties 
'''
def create_map(properties): 
    # generate default map with all properties 
    folium_map = folium.Map(location = [42.3388,-71.0443], zoom_start =15, height=400)

    condition_mapping = {'A': 'Average', 'G': 'Good', 'E': 'Excellent', 'P': 'Poor',
                        'F':'Fair', 'S': 'Special', 'None': 'N/A'}

    type_mapping = {'A': 'Residential 7 or more units', 'CD': 'Residential condimunium UNIT', 
                    'CC': 'Commercial condimunium', 'CM': 'Condomunium main', 'R1':'Residential 1 family', 
                    'R2': 'Residential 2 family', 'R3':'Residential 3 family','R4': 'Residential 4 family or more',
                    'None': 'N/A', 'E': 'Tax Exempt', 'I': 'Industrial', 'C':'Commercial', 'RC': 'Mixed Use', 'AH': 'Agricultural', 
                    'RL': 'Residential Land', 'EA': 'Tax Exempt (121A)', 'CL': 'Commercial Land', 'CP': 'Condo Parking'}

    # go through properties in accessing data, lookup their long/lat in zillow data
    count = 0
    for p in properties: 
        count += 1
        st_num = p['ST_NUM']
        st_name = p['ST_NAME']
        st_suffix = p['ST_NAME_SUF']
        full_addr = st_num + ' ' + st_name + ' ' + st_suffix
        full_addr = ' '.join(full_addr.split())
        price = p['AV_BLDG']
        lat = p['LAT']
        lon = p['LON']
        zipcode = p['ZIPCODE']
        condition = condition_mapping[p['R_OVRALL_CND']] if p['R_OVRALL_CND'] != None else 'N/A'
        type_ = type_mapping[p['LU']] if p['LU'] != None else 'N/A'
        

        if zipcode == '02127':
            s = '''\
            Address: {full_addr}\
            Valuation: {price} \
            Condition: {condition}\
            Type: {type_}\
            '''.format(full_addr=full_addr, price=price, condition = condition, type_ = type_)

            folium.CircleMarker(
            location=[lat,lon],
            radius=0.3,
            color='blue',
            fill=True,
            fill_color='blue').add_child(folium.Popup(s)).add_to(folium_map)

    print('done! ')
    folium_map.save('app/templates/filtered_map.html')
