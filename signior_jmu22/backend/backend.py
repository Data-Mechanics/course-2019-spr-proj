
import requests
import json
import urllib.parse
from flask import Flask 
from flask_cors import CORS
from flask import g
from pymongo import MongoClient




app = Flask(__name__)
CORS(app)

username = urllib.parse.quote_plus('signior_jmu22')
password = urllib.parse.quote_plus('signior_jmu22')

client = MongoClient('mongodb://signior_jmu22:signior_jmu22@127.0.0.1/repo')
db = client['repo']
@app.route("/")
def home():
  return "Backend Up"

@app.route("/statistics")
def statistics():
  result = []
  data = db.signior_jmu22.total_statistical_analysis.find({}, {'_id': 0})
  for row in data:
    result.append(row)
  
  statistics = json.dumps(result)
  return statistics

@app.route("/car_and_emissions")
def car_and_emissions():
  result = []
  data = db.signior_jmu22.constraint_satisfaction2.find({}, {'_id': 0})
  for row in data:
    result.append(row)
  car_and_emissions = json.dumps(result)
  return car_and_emissions

@app.route("/constraint_satisfaction")
def power_plant_emissions():
  result = []
  data = db.signior_jmu22.countries_change_in_carbon_after_year.find({}, {'_id': 0})
  for row in data:
    result.append(row)
  power_plant_emissions = json.dumps(result)
  return power_plant_emissions

@app.route("/statistics2")
def statistics2():
  result = []
  data = db.signior_jmu22.lin_reg2.find({}, {'_id': 0})
  for row in data:
    temp = {}
    country = list(row.keys())[0]
    values = row[country]
    if (values[2] == 1 or values[2] == -1):
      continue
    temp['country'] = country
    temp['slope'] = values[0]
    temp['intercept'] = values[1]
    temp['r_value'] = values[2]
    temp['r_squared'] = values[2] ** 2
    temp['p_value'] = values[3]
    
    result.append(temp)

  statistics2 = json.dumps(result)
  return statistics2
    
  