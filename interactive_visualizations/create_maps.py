import pandas as pd
import math
import dml
import folium
from IPython.display import HTML
import geopandas

# ---[ Assistant Functions ]---------------------------------
def project(R, p):
     return [p(t) for t in R]
# ---[ Connect to Database ]---------------------------------
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('smithnj', 'smithnj')
repo_name = 'smithnj.zones'
# ---[ Initialize Cursor ]-----------------------------------
zones_cursor = repo.smithnj.zones.find()
# ---[ Transformations ]-------------------------------------
zones_data = []
for i in zones_cursor:
     zones_data.append([i["HardshipIndex"], i["Popularity"], i["StationID"], i["TaxiDemand"], i["8Zones"], i["6Zones"], i["4Zones"], i["3Zones"]])
# ---[ Send to Dict ]----------------------------------------
labels = lambda t: {'HardshipIndex': t[0], 'Popularity': t[1], 'StationID': t[2], 'TaxiDemand': t[3], '8Zones': t[4], '6Zones': t[5], '4Zones': t[6], '3Zones': t[7]}
result = project(zones_data, labels)
# ---[ Pandas Transformations ]-------------------------------
df = pd.DataFrame.from_dict(result)
df = df.dropna()
df.StationID = pd.to_numeric(df.StationID)
df = df.set_index("StationID")
df_stationlocation = pd.read_csv('http://datamechanics.io/data/smithnj/CTA_RailStations_CAN.csv') #Re-load station location data.
df_stationlocation["Station Id"] += 40000
df_stationlocation = df_stationlocation.set_index("Station Id")
merged = pd.merge(df, df_stationlocation, left_index=True, right_index=True) # Create a merged dataframe w/ metrics and physical staiton location.t_
merged = merged.reset_index().rename(index=str, columns={"index":"StationID"})
# ---[ Folium ]-----------------------------------------------
def save(map, map_alt):
    map.save("8Zones.html")
    map_alt.save("4Zones.html")
def plotPoints(df, map, map_alt):
    for i in range(len(merged)):
        x = df['Latitude'][i]
        y = df['Longitude'][i]
        zone = df['8Zones'][i]
        zones_alt = df['4Zones'][i]
        colors = ["#B82601", "#813772", "#3CC47C", "#DF744A", "#DCB239", "#4ABDAC", "#015249", "#35A7FF"]
        zone_str = "<b>" + df['Name'][i] + "</b><br>" + "Zone " + str(zones_alt+1) + "<br>(8 Zones)"
        zone_alt_str = "<b>" + df['Name'][i] + "</b><br>" + "Zone " + str(zones_alt+1) + "<br>(4 Zones)"
        marker = folium.CircleMarker(location=[x, y], popup=zone_str, color=colors[zone], fill_color=colors[zone], radius=2)
        marker_alt = folium.CircleMarker(location=[x, y], popup=zone_alt_str, color=colors[zones_alt], fill_color=colors[zones_alt], radius=2)
        marker.add_to(map)
        marker_alt.add_to(map_alt)
folmap = folium.Map(location=[41.8827, -87.6233], zoom_start=11, tiles="CartoDB positron")
folmap_alt = folium.Map(location=[41.8827, -87.6233], zoom_start=11, tiles="CartoDB positron")
folium.GeoJson('/Users/nathaniel/Desktop/CTA_RailLines.geojson', name='geojson').add_to(folmap)
folium.GeoJson('/Users/nathaniel/Desktop/CTA_RailLines.geojson', name='geojson').add_to(folmap_alt)
plotPoints(merged, folmap, folmap_alt)
save(folmap, folmap_alt)