from flask import Flask, request, url_for, render_template
import dml
import pandas as pd


app = Flask(__name__)

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate("aqu1", "aqu1")

@app.route("/", methods = ["GET"])
def get_data():
    
    try:
        boundaries = repo.aqu1.gather_data.find()
        boundaries_output = {
            "type": "FeatureCollection", "features": []}
     
        for row in boundaries:
            boundaries_output["features"].append({"type": "Feature", 
                                                  "properties": {"neighborhood": row["Neighborhood"],
                                                                 "distance": row["Distance"],
                                                                 "percent_bachelors": row["Percent Bachelor\'s Degree"],
                                                                 "percent_low_income": row["Percent Low Income"]},
                                                  "geometry": row["geometry"]
                                                              })

        tstops = repo.aqu1.optimization.find()
        
        stops_output = {"type": "FeatureCollection", "features": []}
        x = 0
        for stop in tstops:
            x += 1
            stops_output["features"].append({"type": "Feature",
                                             "properties": {"point": x},
                                             "geometry": {"type": "Point",
                                                          "coordinates": stop["centroid"]
                                                          }})                                 
        print("got data")
    except:
        print("error")

    return render_template("tstops.html", tstops = stops_output, bounds = boundaries_output)


if __name__ == "__main__":
    app.run(debug=True)
