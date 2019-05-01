from tqdm import tqdm
import json
import jsonschema
from flask import Flask, jsonify, abort, make_response, request, send_from_directory
import pymongo
import dml
import urllib.request
import os
# The project structure references https://github.com/Data-Mechanics/course-2018-spr-proj/tree/master/agoncharova_lmckone


def get_fire_incident_data():
  # Set up the database connection.
  client = dml.pymongo.MongoClient()
  repo = client.repo
  repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')
  fire_incident_data = repo["liweixi_mogujzhu.fire_incident_report"].find()
  fire_count = {}
  for data in fire_incident_data:
    zip_code = "0"+str(data["Zip"])
    if zip_code in fire_count:
      fire_count[zip_code]+=1
    else:
      fire_count[zip_code]=1
  ff_file = open("./data/fire_incident_count.json","w+")
  ff_file.write('var fire_incident_count = ')
  json.dump(fire_count, ff_file)

def download_fire_alarm_boxes_data():
  url = 'https://opendata.arcgis.com/datasets/3a0f4db1e63a4a98a456fdb71dc37a81_1.geojson'
  response = urllib.request.urlopen(url).read().decode("utf-8")
  r = json.loads(response)
  print("Write Boston Fire Alarm boxes to a json file")
  ff_file = open("./data/boston_fire_alarm_boxes.json","w+")
  ff_file.write('var boston_fire_alarm_boxes_json = ')
  json.dump(r, ff_file)

def download_fire_department_data():
  url = 'https://opendata.arcgis.com/datasets/092857c15cbb49e8b214ca5e228317a1_2.geojson'
  response = urllib.request.urlopen(url).read().decode("utf-8")
  r = json.loads(response)
  print("Write Boston Fire Department to a json file")
  ff_file = open("./data/boston_fire_department.json", "w+")
  ff_file.write('var boston_fire_department_json = ')
  json.dump(r, ff_file)

def download_fire_hydrants_data():
  url = 'https://opendata.arcgis.com/datasets/1b0717d5b4654882ae36adc4a20fd64b_0.geojson'
  response = urllib.request.urlopen(url).read().decode("utf-8")
  r = json.loads(response)
  print("Write Boston Fire Hydrants to a json file")
  ff_file = open("./data/boston_fire_hydrants.json", "w+")
  ff_file.write('var boston_fire_hydrants_json = ')
  json.dump(r, ff_file)



app = Flask(__name__)

# serve data
@app.route('/data/<path:path>')
def serve_data(path):
  # get the evictions data from the database and 
  # generate the json file
  print(path + " requested")
  if (path == "boston_fire_department.json") and (not os.path.isfile("./data/boston_fire_department.json")):
    download_fire_department_data()
    print("Generated fire department json file for the map")
  if (path == "boston_fire_hydrants.json") and (not os.path.isfile("./data/boston_fire_hydrants.json")):
    download_fire_hydrants_data()
    print("Generated fire hydrants json file for the map")
  if (path == "boston_fire_alarm_boxes.json") and (not os.path.isfile("./data/boston_fire_alarm_boxes.json")):
    download_fire_alarm_boxes_data()
    print("Generated fire alarm boxes json file for the map")
  if (path == "fire_incident_count.json") and (not os.path.isfile("./data/fire_incident_count.json")):
    get_fire_incident_data()
    print("Count the number of incident ...")
  return send_from_directory('./data', path)

# main css file
@app.route('/css/<path:path>')
def serve_css(path):
  return send_from_directory('./css', path)

# javascript files
@app.route('/js/<path:path>')
def serve_js(path):
  return send_from_directory('./js', path)

@app.route('/', methods=['GET'])
def get_index_page():
  return open('./html/index.html','r').read()

@app.route('/weather_incident', methods=['GET'])
def get_weather_incident_page():
  return open('./html/weather_incident.html','r').read()

@app.route('/fire_count', methods=['GET'])
def get_fire_count_page():
  return open('./html/fire_count.html','r').read()


@app.errorhandler(404)
def not_found(error):
  return make_response(jsonify({'error': 'Not found.'}), 404)

if __name__ == '__main__':
  app.run(debug=True)
