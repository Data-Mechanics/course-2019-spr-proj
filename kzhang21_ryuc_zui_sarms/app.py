import flask
from flask import Flask, Response, request, render_template, redirect, url_for, jsonify
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
import json
import pandas as pd
import requests
from urllib.parse import quote

app = Flask(__name__)

AUTH = json.loads(open("auth.json").read())

API_KEY = AUTH["services"]["Yelp"]["token"]

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.

DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'Boston, MA'
SEARCH_LIMIT = 1

#render index page
@app.route("/", methods=['GET'])
def hello():
    zip_codes = {'02108', '02109', '02110', '02111', '02113', '02114', '02115', '02116', '02118', '02119', '02120',
                '02121', '02122', '02124', '02125', '02126', '02127', '02128', '02129', '02130', '02131', '02132',
                '02133', '02134', '02135', '02136', '02163', '02199', '02203', '02210', '02215', '02222', '02112',
                '02117', '02123', '02137', '02196', '02205', '02283', '02284', '02298', '02201', '02204', '02206',
                '02211', '02212', '02217', '02241', '02266', '02293', '02297'}
    return render_template('index.html', zipCodes= zip_codes)

@app.route("/plot", methods=['POST'])
def plot():
    term = "food"
    zipCode = "02215"
    try:
        zipCode = request.json['data']
        print(zipCode)
    except Exception as e:
        print("shit")
        return "error: "+str(e)   

    result = query_api(term, zipCode)
    print(len(result))

    categories = []
    for business in result:
        categories.extend(business['categories'])
    
    cuisine= []
    for category in categories:
        cuisine.append(category['title'])
    print(len(result))
    
    df = pd.DataFrame({'Cuisines': cuisine})
    dups = df.groupby(['Cuisines'],as_index=True).size().reset_index(name='count')
    dups = dups.sort_values(by='count', ascending=False).head(10)
    print (dups)

    data = [
        go.Bar(
            x=dups['Cuisines'], # assign x as the dataframe column 'x'
            y=dups['count']
        )
    ]

    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    return graphJSON

#---------------------------YELP API----------------------------------#
def apiCall(host, path, api_key, url_params=None):
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }
    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()

def search(api_key, term, location):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+')
    }
    return apiCall(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

def query_api(term, location):
    response = search(API_KEY, term, location)
    businesses = response.get('businesses')
    return businesses