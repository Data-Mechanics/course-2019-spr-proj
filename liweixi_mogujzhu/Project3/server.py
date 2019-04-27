from tqdm import tqdm
import json
import jsonschema
from flask import Flask, jsonify, abort, make_response, request, send_from_directory
import pymongo
import dml
# The project structure references https://github.com/Data-Mechanics/course-2018-spr-proj/tree/master/agoncharova_lmckone



def generate_xxx(year):
    return []

def get_xxx_from_database():
  client = dml.pymongo.MongoClient()
  repo = client.repo
  repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')
  xxx_collection = repo['liweixi_mogujzhu.xxx']
  xxx = xxx_collection.find()
  evictions_arr = []
  years = []
  count = 0
  # TODO Retrive the data



  # format it in the valid format to serve to the map
  repo.logout()
  print("Formatted " + str(count) + " xxx")
  return []

def generate_xxx_json_file():
  '''
  Writes formatted evictions to the file
  '''
  print("Write xxx to a json file")
  xxx_file = open("./data/xxx.json","w")
  xxx_file.write('var xxx_json = {')
  xxx_file.write('"type": "FeatureCollection",')
  xxx_file.write('"features":')

  xxx = get_xxx_from_database()
  xxx_file.write(json.dumps(xxx))
  xxx_file.write("}")


app = Flask(__name__)

# serve data
@app.route('/data/<path:path>')
def serve_data(path):
  # get the evictions data from the database and 
  # generate the json file
  print(path + " requested")
  if(path == "xxx.json"):
    generate_xxx_json_file()
    print("Generated xxx json file for the map")
  if(path == "xxx2.json"):
    generate_xxx_json_file()
    print("Generated xxx2 json file for the graph")
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

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found.'}), 404)

if __name__ == '__main__':
    app.run(debug=True)
