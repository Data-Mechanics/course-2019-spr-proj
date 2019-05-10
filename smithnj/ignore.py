import json
import prov.model
import datetime
import uuid
import dml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sklearn
from sklearn.cluster import KMeans
from mpl_toolkits.mplot3d import Axes3D
from sklearn.preprocessing import scale
import sklearn.metrics as sm
import seaborn as sns
from scipy.stats.stats import pearsonr

# ---[ Assistant Functions ]---------------------------------
def project(R, p):
    return [p(t) for t in R]


startTime = datetime.datetime.now()
# ---[ Connect to Database ]---------------------------------
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('smithnj', 'smithnj')
# ---[ Initialize Cursors ]----------------------------------
zones_cursor = repo.smithnj.zones.find()
# ---[ Staging & Scaling ]-----------------------------------
metrics_data = []
for i in zones_cursor:
    metrics_data.append((i["StationID"], i["Popularity"], i["HardshipIndex"], i["TaxiDemand"]))
popularity = [b for (a, b, c, d) in metrics_data if b is not None]
hardship = [c for (a, b, c, d) in metrics_data if c is not None]
taxidemand = [d for (a, b, c, d) in metrics_data if d is not None]
# ---[ Stats Operations ]-----------------------------------
Corr_popularity_hardship = pearsonr(popularity[0:123], hardship[0:123])
Corr_popularity_taxi = pearsonr(popularity[0:123], taxidemand[0:123])
Corr_hardship_taxi = pearsonr(hardship[0:123], taxidemand[0:123])
print("Popularity and Hardship Correlation Coeff:" + str(Corr_popularity_hardship[0]))
print("Popularity and Taxi Demand Correlation Coeff:" + str(Corr_popularity_taxi[0]))
print("Hardship and Taxi Demand Correlation Coeff: " + str(Corr_hardship_taxi[0]))
# ---[ Finishing Up ]---------------------------------------
repo.logout()
endTime = datetime.datetime.now()