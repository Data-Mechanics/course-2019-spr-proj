import urllib.request
import json
import dml
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask_pymongo import PyMongo

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def homePage():
    if request.method == "GET":
        return render_template("HomePage.html")


@app.route("/showSchool", methods=["GET"])
def getSchool():
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('yufeng72', 'yufeng72')
    schoolList = repo['yufeng72.stationNearbySchool'].find()
    schoolResult = []
    for row in schoolList:
        name = row['Name']
        lati = row['Latitude']
        long = row['Longitude']
        numb = row['HubwayStationNearby']
        schoolResult.append([name, lati, long, numb])
    repo.logout()
    return render_template("ShowSchool.html", schools=schoolResult)


@app.route("/showStation", methods=["GET"])
def getStation():
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('yufeng72', 'yufeng72')
    stationList = repo['yufeng72.hubwayStations'].find()
    stationResult = []
    for row in stationList:
        name = row['StationID']
        lati = row['Latitude']
        long = row['Longitude']
        stationResult.append([name, lati, long])
    repo.logout()
    return render_template("ShowStation.html", stations=stationResult)


@app.route("/showAll", methods=["GET"])
def getAll():
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('yufeng72', 'yufeng72')
    stationList = repo['yufeng72.hubwayStations'].find()
    stationResult = []
    for row in stationList:
        name = row['StationID']
        lati = row['Latitude']
        long = row['Longitude']
        stationResult.append([name, lati, long])
    schoolList = repo['yufeng72.stationNearbySchool'].find()
    schoolResult = []
    for row in schoolList:
        name = row['Name']
        lati = row['Latitude']
        long = row['Longitude']
        numb = row['HubwayStationNearby']
        schoolResult.append([name, lati, long, numb])
    repo.logout()
    return render_template("ShowAll.html", stations=stationResult, schools=schoolResult)


@app.route("/placeNew", methods=["GET"])
def addNew():
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('yufeng72', 'yufeng72')
    stationList = repo['yufeng72.hubwayStations'].find()
    stationResult = []
    for row in stationList:
        name = row['StationID']
        lati = row['Latitude']
        long = row['Longitude']
        stationResult.append([name, lati, long])
    schoolList = repo['yufeng72.stationNearbySchool'].find()
    schoolResult = []
    for row in schoolList:
        name = row['Name']
        lati = row['Latitude']
        long = row['Longitude']
        numb = row['HubwayStationNearby']
        schoolResult.append([name, lati, long, numb])
    repo.logout()
    return render_template("PlaceNew.html", stations=stationResult, schools=schoolResult)



if __name__ == '__main__':
    app.run()
