from flask import (
  Flask, 
  render_template, 
  jsonify,
  abort
)
import dml

app = Flask(__name__, static_folder='public', static_url_path='')

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('kgarber', 'kgarber')

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/api/getData/<string:dataname>')
def getData(dataname):
  if dataname == "bluebike,numrides":
    rides_and_weather = repo['kgarber.bluebikes.rides_and_weather'].find()
    days = [d for d in rides_and_weather]
    for d in days:
      del d["_id"]
    ride_weather_stats = repo['kgarber.bluebikes.ride_weather_stats'].find()
    stats = [s for s in ride_weather_stats]
    for s in stats:
      del s["_id"]
    return jsonify({
      "data": days,
      "stats": stats
    })
  elif dataname == "bluebike,new-station":
    stations = repo['kgarber.bluebikes.stations'].find()
    stations = [s for s in stations]
    new_station = repo['kgarber.bluebikes.new_station'].find()
    new_station = [n for n in new_station]
    for s in stations:
      del s["_id"]
    for n in new_station:
      del n["_id"]
    return jsonify({
      "stations": stations,
      "new_station": new_station
    })
  elif dataname == "bluebike,BU":
    stations = repo['kgarber.bluebikes.stations'].find()
    stations = [s for s in stations]
    for s in stations:
      del s["_id"]
    bu = repo['kgarber.university_bluebike_stations'].find_one({
      "properties.Name": "Boston University"
    })
    del bu["_id"]
    nearby_stat = bu["nearbyStations"]
    nearby = [s for s in stations if s["stationId"] in nearby_stat]
    common_stat = [c["_id"] for c in bu["commonStations"]]
    common = [s for s in stations if s["stationId"] in common_stat]
    return jsonify({
      "BU": bu,
      "NEARBY": nearby,
      "COMMON": common
    })
  return abort(404)
