import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D


def project(R, p):
    return [p(t) for t in R]


def product(R, S):
    return [(t, u) for t in R for u in S]

startTime = datetime.datetime.now()

# ---[ Connect to Database ]---------------------------------
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('smithnj', 'smithnj')
repo_name = 'smithnj.metrics'
# ---[ Initialize Cursors ]----------------------------------
metrics_cursor = repo.smithnj.metrics.find()
# ---[ Transformations ]-------------------------------------
metrics_list = []
for i in metrics_cursor:
    metrics_list.append((i["Station ID"], i["Popularity"], i["Hardship Index"], i["Taxi Demand"]))
# ---[ Send to Dict ]----------------------------------------
labels = lambda t: {'Station ID': t[0], 'Popularity': t[1], 'Hardship Index': t[2], 'Taxi Demand': t[3]}
result = project(metrics_list, labels)
# ---[ MongoDB Insertion ]-----------------------------------
df = pd.DataFrame.from_dict(result)
df.set_index("Station ID", inplace=True)
df.dropna()
threedee = plt.figure().gca(projection='3d')
threedee.scatter(df['Hardship Index'], df['Popularity'], df['Taxi Demand'])
threedee.set_xlabel('Hardship Index')
threedee.set_ylabel('Popularity')
threedee.set_zlabel('Taxi Demand')
plt.show()
plot = df.plot()
fig = plot.get_figure()
fig.savefig("/Users/nathaniel/Desktop/yay.png")
# repo.dropCollection(repo_name)
# repo.createCollection(repo_name)
# print('done')
# repo[repo_name].insert_many(loaded)
# repo[repo_name].metadata({'complete': True})
# # ---[ Finishing Up ]-------------------------------------------
# print(repo[repo_name].metadata())
repo.logout()
# endTime = datetime.datetime.now()

