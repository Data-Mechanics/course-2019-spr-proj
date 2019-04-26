# import urllib.request
# import json
# import dml
# from flask_pymongo import PyMongo
from flask import Flask, render_template, request
from flask_googlemaps import GoogleMaps, Map
from config import getGoogleMapsKey

app = Flask(__name__, template_folder="templates")
app.config['GOOGLEMAPS_KEY'] = getGoogleMapsKey()
GoogleMaps(app)

@app.route('/')
def mapview():

    mymap = Map(
        identifier="view-side",
        lat=42.3601,
        lng=-71.0589,
        markers=[(42.3601, -71.0589)]
    )
    sndmap = Map(
        identifier="sndmap",
        lat=42.3601,
        lng=-71.0589,
        markers=[
            {
                'icon': 'http://maps.google.com/mapfiles/ms/icons/green-dot.png',
                'lat': 42.3601,
                'lng': -71.0589,
                'infobox': "<b>Hello World</b>"
            },
            {
                'icon': 'http://maps.google.com/mapfiles/ms/icons/blue-dot.png',
                'lat': 42.3601,
                'lng': -71.0589,
                'infobox': "<b>Hello World from other place</b>"
            }
        ]
    )
    url = "https://maps.googleapis.com/maps/api/js?key=" + getGoogleMapsKey() + "&libraries=places&callback=initMap"

    # print(url)
    return render_template("index.html", url=url)
    # mymap=mymap, sndmap=sndmap,


@app.route('/neighborhood', methods=['GET', 'POST'])
def neighborhood():
    nhood = request.form.get('neighborhoods')

    return render_template("index.html", key=getGoogleMapsKey(), neighborhood=nhood)


if __name__ == '__main__':
    app.run('localhost', 8000, debug=True)
