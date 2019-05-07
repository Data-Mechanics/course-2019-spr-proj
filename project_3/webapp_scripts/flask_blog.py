from flask import Flask, render_template, url_for
import pandas as pd
app = Flask(__name__)

proteins = [
    {
        'pdbid': '1qfw',
        'casetype': 'ab',
        'coefnum': '104',
        'modrank': 844,
        'clusrank': 25,
        'fnat': 0,
        'irms': 12.644,
        'lrms': 47.655,
        'dockq': 0.015,
        'tot_energy': -903.68,
        'es1': 328,
        'es2': 1350,
        'vdw1': -0.1233,
        'vdw2': -0.8528,
        'dars': -397.5,
    },
    { 
        'pdbid': '1qfw',
        'casetype': 'ab',
        'coefnum': '103',
        'modrank': 219,
        'clusrank': 32,
        'fnat': 0.736,
        'irms': 2.324,
        'lrms': 11.942,
        'dockq': 0.455,
        'tot_energy': -840.19,
        'es1': 132,
        'es2': 1362,
        'vdw1': -0.1307,
        'vdw2': -0.6922,
        'dars': -295.2,
    }
]

@app.route('/')
@app.route('/home')
def home():
    return render_template('home.html', data=proteins)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/data/newpip_oldgrp_iter1_4eigs_res_and_energy')
def show_data():
    dataframe = pd.read_csv("data/newpip_oldgrp_iter1_4eigs_res_and_energy")
    res_json = dataframe.to_json(orient='records')
    #return open("data/newpip_oldgrp_iter1_4eigs_res_and_energy.txt").read()
    return res_json

@app.route('/data/class.csv')
def show_bm5classes():
    bm5_df = pd.read_csv("data/class.csv")
    class_json = bm5_df.to_json(orient='records')
    return class_json

@app.route('/utils.js')
def utils_for_chart():
    return render_template("utils.js")
'''
@app.route('/giphy.gif')
def load_loadingiphy():
    return open("giphy.gif").read()
'''
@app.route('/bubble_trial.html')
def bubble_apifig():
    return render_template("bubble_trial.html")

@app.route('/bubble_ab_ag.html')
def bubble_fig():
    return render_template("bubble_ab_ag.html")

@app.route('/cluster_trial.html')
def cluster_fig():
    return render_template("cluster_trial.html")

@app.route('/donut_trial.html')
def donut_fig():
    return render_template("donut_trial.html")
'''
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    return 'You want path: %s' % path
'''
if __name__ == '__main__':
    app.run(debug=True)
