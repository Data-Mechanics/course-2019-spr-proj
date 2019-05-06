from flask import Blueprint, render_template, abort, request, jsonify, current_app, send_from_directory
from jinja2 import TemplateNotFound


import pandas as pd
import json

import plotly
import plotly.plotly as py
import plotly.graph_objs as go


from web import db

cs504 = Blueprint('cs504', __name__,
                  template_folder='templates',
                  static_folder=''
                  )


ZIP_CODES = {'02108', '02109', '02110', '02111', '02113', '02114', '02115', '02116', '02118', '02119', '02120',
             '02121', '02122', '02124', '02125', '02126', '02127', '02128', '02129', '02130', '02131', '02132',
             '02133', '02134', '02135', '02136', '02163', '02199', '02203', '02210', '02215', '02222', '02112',
             '02117', '02123', '02137', '02196', '02205', '02283', '02284', '02298', '02201', '02204', '02206',
             '02211', '02212', '02217', '02241', '02266', '02293', '02297'}
VIOLATIONS = {
    "Allston": 0.0148305112,
    "Back Bay": 0.014673516,
    "Bay Village": 0.014673516,
    "Beacon Hill": 0.0079316101,
    "Brighton": 0.013214928,
    "Charlestown": 0.0111358323,
    "Chinatown": 0.0142877959,
    "Leather District": 0.0142877959,
    "Dorchester": 0.0200310624,
    "Downtown": 0.0110069338,
    "East Boston": 0.0124370158,
    "Fenway": 0.0148607211,
    "Longwood": 0.0148607211,
    "Hyde Park": 0.0189047565,
    "Jamaica Plain": 0.0157670894,
    "Mattapan": 0.0200543367,
    "Mission Hill": 0.0266863733,
    "North End": 0.0133527343,
    "Roslindale": 0.0209122753,
    "Roxbury": 0.0202292164,
    "South Boston": 0.0116010752,
    "South Boston Waterfront": 0.0146368075,
    "South End": 0.0187959066,
    "West End": 0.0187833246,
    "West Roxbury": 0.0201359561
}
RATINGS = {
    "Allston": 3.6875,
    "Back Bay": 3.7775229358,
    "Bay Village": 3.7775229358,
    "Beacon Hill": 3.6938202247,
    "Brighton": 3.7681818182,
    "Charlestown": 3.6351351351,
    "Chinatown": 3.6461864407,
    "Leather District": 3.6461864407,
    "Dorchester": 3.4085106383,
    "Downtown": 3.5961538462,
    "East Boston": 3.2379182156,
    "Fenway": 3.5431937173,
    "Longwood": 3.5431937173,
    "Hyde Park": 3.3333333333,
    "Jamaica Plain": 3.7987012987,
    "Mattapan": 3.2878787879,
    "Mission Hill": 3.59375,
    "North End": 3.8477508651,
    "Roslindale": 3.2448979592,
    "Roxbury": 3.5316455696,
    "South Boston": 3.8106060606,
    "South Boston Waterfront": 3.5472972973,
    "South End": 3.8975903614,
    "West End": 3.7417582418,
    "West Roxbury": 3.6826923077
}


@cs504.route('/')
def index():
    return send_from_directory("templates", "index.html")


@cs504.route("/info/<neigh>")
def neigh_info(neigh):
    r = {
        "violationRate": "%.4f" % VIOLATIONS.get(neigh),
        "averageRating": "%.4f" % RATINGS.get(neigh)
    }
    return jsonify(r)


@cs504.route("/yelpPlot/<neigh>", methods=["GET"])
def yelp_plot(neigh):
    # neigh = request.args.get("neigh", None)
    zip_code = request.args.get("zip", None)
    if neigh:
        rests = db.cat_by_neigh(neigh)
    else:

        rests = db.cat_by_zip(zip_code)

    categories = []
    for business in rests:
        categories.extend(business['categories'])

    cuisine = []
    for category in categories:
        cuisine.append(category['title'])
    print(len(rests))

    df = pd.DataFrame({'Cuisines': cuisine})
    dups = df.groupby(['Cuisines'], as_index=True).size(
    ).reset_index(name='count')
    dups = dups.sort_values(by='count', ascending=True).tail(10)
    # print(dups)

    data = [
        go.Bar(
            x=dups['count'],  # assign x as the dataframe column 'x'
            y=dups['Cuisines'],
            name=zip_code if zip_code else neigh,
            orientation="h"
        )
    ]

    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    return current_app.response_class(graphJSON, mimetype=current_app.config['JSONIFY_MIMETYPE'])
