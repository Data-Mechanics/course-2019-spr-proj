from flask import Flask, render_template, request, jsonify, abort, make_response
from pymongo import MongoClient
import dml

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('misn15', 'misn15')

app = Flask(__name__)

@app.route('/', methods = ["GET"])
def welcome():
    return render_template('welcome.html')

@app.route('/maps', methods = ["GET"])
def maps():
    return render_template('leaflet.html')

@app.route('/heat_maps', methods = ['GET','POST'])
def heat_maps():
    if request.method == 'POST':     
        if request.form["submit_button"] == "Waste":     
            waste = list(repo['misn15.waste_all'].find()) 
            waste_list = []
            for x in waste:
                coords = x['Coordinates']
                waste_list += [[coords[1], coords[0]]]        
            return render_template('boston_map.html', data = waste_list)
        elif request.form["submit_button"] == "Crime": 
            crime = list(repo['misn15.crime'].find())  
            crime_list = []
            for x in crime:
                if x['Lat'] != None:
                    crime_list += [[x['Lat'], x['Long']]]  
            return render_template('boston_map.html', data = crime_list)
        elif request.form["submit_button"] == "Open Spaces":
            open_space = list(repo['misn15.openSpace_centroids'].find())
            open_space_list = []
            for x in open_space:
                coords = x['Coordinates']
                open_space_list += [[coords[1], coords[0]]]
            return render_template('boston_map.html', data = open_space_list)
    else:
        waste = list(repo['misn15.waste_all'].find()) 
        waste_list = []
        for x in waste:
            coords = x['Coordinates']
            waste_list += [[coords[1], coords[0]]]
        return render_template('boston_map.html', data = waste_list)

@app.route('/graph_data', methods = ['GET','POST']) 
def graph():
    data = list(repo['misn15.crime_health_waste_space'].find())
    if request.method == 'POST':  
        if request.form["submit_button"] == "Waste":         
            data_list = []
            for x in data:
                data_list += [[x['total occurrences'], x['waste']]]        
            return render_template('scatter_v3.html', data = data_list, label = 'Waste Sites')
        elif request.form["submit_button"] == "Income": 
            data_list = []
            for x in data:
                data_list += [[x['total occurrences'], x['income']]] 
            return render_template('scatter_v3.html', data = data_list, label = 'Income')
        elif request.form["submit_button"] == "Open Spaces":
            data_list = []
            for x in data:
                data_list += [[x['total occurrences'], x['open space']]]
            return render_template('scatter_v3.html', data = data_list, label = 'Open Spaces')
        elif request.form["submit_button"] == "Crime": 
            data_list = []
            for x in data:
                data_list += [[x['total occurrences'], x['crime']]] 
            return render_template('scatter_v3.html', data = data_list, label = 'Crimes')        
    else:         
        data_list = []
        for x in data:
            data_list += [[x['total occurrences'], x['waste']]]        
        return render_template('scatter_v3.html', data = data_list, label = 'Waste Sites')

@app.route('/disease', methods=["GET", "POST"])
def disease():
    optimal= list(repo['misn15.waste_optimal'].find())
    opt_coords = []
    for x in optimal:
        opt_coords += [[[x['Coordinates'][1], x['Coordinates'][0]], x['Rank']]]
    return render_template("choropleth_disease2.html", data = opt_coords)

# expose data
@app.route('/api', methods = ['GET']) 
def api_page():
    return render_template("api_page.html")
    
@app.route('/api/<path:path>', methods = ['GET']) 
def serve_data(path):
    if(path == "health"):
        data = list(repo['misn15.clean_health'].find()) 
        health_list = []
        for x in data:
            formatted_health = {'tractfips': x['tractfips'], 'population': x['populationcount'], 'Latitude': x['coordinates'][1], 'Longitude': x['coordinates'][0], 'High Blood Pressure': x['High Blood Pressure'],
                'High Cholesterol': x['High Cholesterol'], 'COPD': x['COPD'], 'Arthritis': x['Arthritis'], 'Chronic Kidney Disease': x['Chronic Kidney Disease'],
                'Diabetes': x['Diabetes'], 'Mental Health': x['Mental Health'], 'Cancer (except skin)': x['Cancer (except skin)'], 'Physical Health': x['Physical Health'],
                'Teeth Loss': x['Teeth Loss'], 'Stroke': x['Stroke'], 'Coronary Heart Disease': x['Coronary Heart Disease'], 'Asthma': x['Current Asthma']}
            health_list.append(formatted_health)
        return jsonify(health_list)
    if(path == "waste"):
        data = list(repo['misn15.waste_all'].find())
        waste_list = []
        for x in data:
            formatted_waste = {'Name': x['Name'], 'Address': x['Address'], 'Latitude': x['Coordinates'][1], 'Longitude': x['Coordinates'][0],
                            'Zip code': x['Zip Code'], 'Latitude': x['Coordinates'][1], 'Longitude': x['Coordinates'][0], 'Status': x['Status'],
                            'FIPS': x['FIPS']}
            waste_list.append(formatted_waste)
        return jsonify(health_list)
    elif(path == "crime"):
        data = list(repo['misn15.crime'].find())
        crime_list = []
        for x in data:
            formatted_crime = {'Street': x['STREET'], 'Offense Description': x['OFFENSE_DESCRIPTION'],'Occurred On': x['OCCURRED_ON_DATE'], 'Longitude': x['Long'],
                            'Latitude': x['Lat'], 'Offense Code Group': x['OFFENSE_CODE_GROUP']}
            crime_list.append(formatted_crime)
        return jsonify(crime_list)
    elif(path == "open_spaces"):
        data = list(repo['misn15.openSpace_centroids'].find())
        open_space_list = []
        for x in data:
            formatted_open_space = {'Name': x['Name'], 'Latitude': x['Coordinates'][1],'Longitude': x['Coordinates'][0],'FIPS': x['FIPS']}
            open_space_list.append(formatted_open_space)
        return jsonify(open_space_list)
    else:
        return not_found(404)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not a valid request'}), 404)

@app.route('/leaflet_dots', methods = ["GET", "POST"])
def leaflet_dots():
    return render_template("leaflet_dots.html")

@app.route('/waste_data', methods = ["GET", "POST"])
def waste_data():
    data = list(repo['misn15.waste_all'].find()) 
    coords = []
    for x in data:
        points = x['Coordinates']
        coords += [[points[1], points[0]]]
    return jsonify(coords)

@app.route('/openSpace_data', methods = ["GET", "POST"])
def openSpace_data():
    data = list(repo['misn15.openSpace_centroids'].find()) 
    coords = []
    for x in data:
        points = x['Coordinates']
        coords += [[points[1], points[0]]]
    return jsonify(coords)


if __name__ == '__main__':
    app.run(debug=False)
