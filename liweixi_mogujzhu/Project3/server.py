from tqdm import tqdm
import json
import jsonschema
from flask import Flask, jsonify, abort, make_response, request, send_from_directory
import pymongo
import dml
import urllib.request
import os
import matplotlib.pyplot as plt
import numpy as np
import base64
from flask import render_template
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

def drawFromDate(date):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')
    data_name = 'liweixi_mogujzhu.weather_fire_incident_transformation'

    # initialize fields in weather_fire_incident_transformation
    size = 31
    TAVG = np.zeros(size)
    AWND = np.zeros(size)
    PRCP = np.zeros(size)
    SNOW = np.zeros(size)
    NINCIDENT = np.zeros(size)

    dateStart = date + "-01"
    dateEnd = date + "-32"
    i = 0
    # retrieve data from 2017-01-01 to 2017-01-31
    for col in repo[data_name].find({"DATE":{"$gte":dateStart,"$lt":dateEnd}}):
        TAVG[i] = col['TAVG']
        AWND[i] = col['AWND'] * 1.61
        PRCP[i] = col['PRCP'] * 2.54 * 7
        SNOW[i] = col['SNOW'] * 2.54 * 7
        NINCIDENT[i] = col['NINCIDENT']
        i += 1

    # print(TAVG)
    # print(AWND)
    # print(PRCP)
    # print(SNOW)
    # print(NINCIDENT)

    n = 5   # number of bars in one day
    total_width = 0.8   # set total width of the bars
    plt.rcParams["figure.figsize"] = [14,8] # set size of the figure

    # compute the width and position of every bar
    x = np.arange(size) + 1
    width = total_width / n # width for every bar
    x = x - (total_width - width) / 2
    # draw 5 bars per day
    plt.bar(x, TAVG,  width=width, label='TAVG: ℉')
    plt.bar(x + width, AWND,  width=width, label='AWND: km/h')
    plt.bar(x + 2*width, PRCP,  width=width, label='PRCP: mm')
    plt.bar(x + 3*width, SNOW,  width=width, label='SNOW: mm')
    plt.bar(x + 4*width, NINCIDENT, width=width, label='NINCIDENT')
    # set x-axis
    x_axis = np.arange(1,32,1)
    plt.xticks(x_axis)

    plt.title(date + " Weather_Incident")  # set title
    plt.legend()
    # plt.figure(figsize=(10,8))
    plt.savefig("./static/weather_incident_histogram.png")
    # plt.show()


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

@app.route('/fire_count', methods=['GET'])
def get_fire_count_page():
  return open('./html/fire_count.html','r').read()

@app.route('/histogram/<string:date>', methods=['GET', 'POST'])
def get_date_histogram(date):
  if request.method == 'POST':
        time = request.form.get('Date')
        print(time)

  pathHis = "../static/weather_incident_histogram_" + date + ".png"
  print(pathHis)
  return render_template('weather_incident.html',img_path=pathHis)

@app.errorhandler(404)
def not_found(error):
  return make_response(jsonify({'error': 'Not found.'}), 404)

if __name__ == '__main__':
  app.run(debug=True)
