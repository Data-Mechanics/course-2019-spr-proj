# import urllib.request
# import json
# import dml
# from flask_pymongo import PyMongo
from flask import Flask, render_template, request
import flask
from flask_googlemaps import GoogleMaps, Map
from config import getGoogleMapsKey

app = Flask(__name__, template_folder="templates")
app.config['GOOGLEMAPS_KEY'] = getGoogleMapsKey()
GoogleMaps(app)

@app.route('/')
def mapview():

    url = "https://maps.googleapis.com/maps/api/js?key=" + getGoogleMapsKey() + "&libraries=places&callback=initMap"

    return render_template("index.html", url=url)

@app.route('/about')
def about():
    return render_template("about.html")

@app.route('/neighborhood', methods=['GET', 'POST'])
def neighborhood():
    nhood = flask.request.form.get('neighborhoods')
    print(nhood)

    url = "https://maps.googleapis.com/maps/api/js?key=" + getGoogleMapsKey() + "&libraries=places&callback=initMap"

    return render_template("search.html", url=url, neighborhood=nhood)


if __name__ == '__main__':
    app.run('localhost', 8000, debug=True)
