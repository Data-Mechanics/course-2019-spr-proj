# import urllib.request
# import json
# import dml
# from flask_pymongo import PyMongo
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask_googlemaps import GoogleMaps, Map
from config import getGoogleMapsKey

app = Flask(__name__, template_folder="templates")
app.config['GOOGLEMAPS_KEY'] = getGoogleMapsKey()
GoogleMaps(app)

# users = [
#     {'id': 1, 'username': u'kgrewal'},
#     {'id': 2, 'username': u'shin2'}
# ]
#
# schema = {
#     "type": "object",
#     "properties": {"username": {"type": "string"}},
#     "required": ["username"]
# }


@app.route('/')
def mapview():

    mymap = Map(
        identifier="view-side",
        lat=37.4419,
        lng=-122.1419,
        markers=[(37.4419, -122.1419)]
    )
    sndmap = Map(
        identifier="sndmap",
        lat = 37.4419,
        lng= -122.1419,
        markers=[
            {
                'icon': 'http://maps.google.com/mapfiles/ms/icons/green-dot.png',
                'lat': 37.4419,
                'lng': -122.1419,
                'infobox': "<b>Hello World</b>"
            },
            {
                'icon': 'http://maps.google.com/mapfiles/ms/icons/blue-dot.png',
                'lat': 37.4300,
                'lng': -122.1400,
                'infobox': "<b>Hello World from other place</b>"
            }
        ]
    )
    return render_template("index.html", mymap=mymap, sndmap=sndmap)




if __name__ == '__main__':
    app.run('localhost', 8000, debug=True)
