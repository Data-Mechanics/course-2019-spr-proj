import flask
from flask import Flask, Response, request, render_template, redirect, url_for
#import plotly
#plotly.tools.set_credentials_file(username='insert', api_key='insert')

app = Flask(__name__)

app.config["CACHE_TYPE"] = "null"

#render index page
@app.route("/", methods=['GET'])
def hello():
    zip_codes = {'02108', '02109', '02110', '02111', '02113', '02114', '02115', '02116', '02118', '02119', '02120',
                     '02121', '02122', '02124', '02125', '02126', '02127', '02128', '02129', '02130', '02131', '02132',
                     '02133', '02134', '02135', '02136', '02163', '02199', '02203', '02210', '02215', '02222', '02112',
                     '02117', '02123', '02137', '02196', '02205', '02283', '02284', '02298', '02201', '02204', '02206',
                     '02211', '02212', '02217', '02241', '02266', '02293', '02297'}
    return render_template('index.html', zipCodes= zip_codes)