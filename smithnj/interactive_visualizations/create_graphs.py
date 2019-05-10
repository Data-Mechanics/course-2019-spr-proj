import plotly
import pandas as pd
# import dml
import pymongo

# ---[ Assistant Functions ]---------------------------------
def project(R, p):
     return [p(t) for t in R]
# ---[ Connect to Database ]---------------------------------
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('smithnj', 'smithnj')
repo_name = 'smithnj.kmeans.data'
# ---[ Initialize Cursor ]-----------------------------------
kmeans_data_cursor = repo.smithnj.kmeans.data.find()
kmeans_centers_cursor = repo.smithnj.kmeans.centers.find()
# ---[ Send to DataFrames ]-----------------------------------
data = []
centers = []
for i in kmeans_centers_cursor:
    centers.append([i["NumCenters"], i["Popularity"], i["HardshipIndex"], i["TaxiDemand"]])
for i in kmeans_data_cursor:
    data.append([i["StationID"], i["S_Popularity"], i["S_HardshipIndex"], i["S_TaxiDemand"], i["8Zones"], i["6Zones"], i["4Zones"], i["3Zones"]])
data_labels = lambda t: {'StationID': t[0], 'Popularity': t[1], 'HardshipIndex': t[2], 'TaxiDemand': t[3], '8Zones': t[4], '6Zones': t[5], '4Zones': t[6], '3Zones': t[7]}
centers_labels = lambda t: {'NumCenters': t[0], 'Popularity': t[1], 'HardshipIndex': t[2], 'TaxiDemand': t[3]}
data = project(data, data_labels)
centers = project(centers, centers_labels)
df_centers = pd.DataFrame.from_dict(centers)
df_data = pd.DataFrame.from_dict(data)

def getColor(row, access):
    colors = ["#B82601", "#813772", "#3CC47C", "#DF744A", "#DCB239", "#4ABDAC", "#015249", "#35A7FF"]
    return colors[row[access]]

colors = ["#B82601", "#813772", "#3CC47C", "#DF744A", "#DCB239", "#4ABDAC", "#015249", "#35A7FF"]
zones = ['8 clusters/zones', '6 clusters/zones', '4 clusters/zones', '3 clusters/zones']
ref = ['8Zones', '6Zones', '4Zones', '3Zones']

for i in range(len(zones)):
    df_zones = df_centers.loc[df_centers['NumCenters'] == zones[i]].reset_index()
    df_data['Color'] = df_data.apply(lambda row: getColor(row, ref[i]), axis=1)
    colors = colors[0: len(df_zones)]
    df_colors = pd.DataFrame(colors)
    df_colors.columns = ["Color"]
    df_zones = df_zones.join(df_colors)

    scatter = dict(
        mode="markers",
        name="Stations",
        type="scatter3d",
        x=df_data['Popularity'], y=df_data['HardshipIndex'], z=df_data['TaxiDemand'],
        marker=dict(size=3, color=df_data['Color']), text="Station #" + df_data['StationID']
    )

    clusters = dict(
        mode="markers",
        name="Zone/Cluster Centers",
        type="scatter3d",
        x=df_zones['HardshipIndex'], y=df_zones['Popularity'], z=df_zones['TaxiDemand'],
        marker=dict(size=10, color=df_zones['Color']), text="Cluster"
    )

    layout = dict(
        title=zones[i],
        scene=dict(
            xaxis=dict(
                title='Popularity',
            ),
            yaxis=dict(
                title='Hardship Index',
            ),
            zaxis=dict(
                title='Taxi Demand',
            )
        )
    )

    fig = dict(data=[scatter, clusters], layout=layout)
    plotly.offline.plot(fig, auto_open=True, filename=ref[i]+"-Graph.html")