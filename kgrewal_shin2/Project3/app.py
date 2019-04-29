import dml
from flask import Flask, render_template, request
import flask
from flask_googlemaps import GoogleMaps
from config import getGoogleMapsKey
import random

app = Flask(__name__, template_folder="templates")
app.config['GOOGLEMAPS_KEY'] = getGoogleMapsKey()
GoogleMaps(app)

@app.route('/', methods=['GET', 'POST'])
def mapview():
    return render_template("about.html")


@app.route('/mapping')
def clust():
    url = "https://maps.googleapis.com/maps/api/js?key=" + getGoogleMapsKey() + "&callback=initMap"
    print(url)
    return render_template("mapping.html", url=url)

@app.route('/map')
def about():
    url = "https://maps.googleapis.com/maps/api/js?key=" + getGoogleMapsKey() + "&libraries=places&callback=initMap"

    return render_template("index.html", url=url)

@app.route('/neighborhood', methods=['GET', 'POST'])
def neighborhood():
    nhood = flask.request.form.get('neighborhoods')
    print(nhood)
    url = "https://maps.googleapis.com/maps/api/js?key=" + getGoogleMapsKey() + "&libraries=places&callback=initMap"

    if request.method == 'POST':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')
        neigh_streets = list(repo['kgrewal_shin2.neigh_streets'].find())

        streets = []
        for s in neigh_streets:
            if s['neighborhood'] == nhood and s['gender']=='male':
                streets.append(s['full_name'])
        if len(streets) == 0:
            streets.append("No Street Suggestions Found")
        repo.logout()

        streets2 = []
        if len(streets) > 15:
            while len(streets2) < 15:
                st = random.choice(streets)
                if st not in streets2:
                    if st.endswith('wa') or st.endswith('Wa'):
                        st = st + 'y'
                    streets2.append(st)

    return render_template("search.html", url=url, neighborhood=nhood, streets=streets2)


@app.route('/street', methods=['GET', 'POST'])
def street():
    result = flask.request.form
    nhood = result['neighborhood']

    if request.method == 'POST':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')
        neigh_streets = list(repo['kgrewal_shin2.neigh_streets'].find())

        streets = []
        for s in neigh_streets:
            if s['neighborhood'] == nhood and s['gender'] == 'male':
                streets.append(s['full_name'])
        if len(streets) == 0:
            streets.append("No Street Suggestions Found")
        repo.logout()

    streets2 = []
    if len(streets) > 15:
        while len(streets2) < 15:
            st = random.choice(streets)
            if st not in streets2:
                if st.endswith('wa') or st.endswith('Wa'):
                    st = st + 'y'
                streets2.append(st)

    url = "https://maps.googleapis.com/maps/api/js?key=" + getGoogleMapsKey() + "&libraries=places&callback=initMap"

    try:
        street = result['street']
    except:
        render_template(render_template("search.html", url=url, neighborhood=nhood, streets=streets2))

    return render_template("stsearch.html", url=url, street=street, neighborhood=nhood, streets=streets2)


if __name__ == '__main__':
    app.run('localhost', 8000, debug=True)
