import folium
from folium import plugins
import dml

reads = ['henryhcy_jshen97_leochans_wangyp.neighborhoods']
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')

def Mapping():
   

    m = folium.Map(location = [42.3601,-71.0589],zoom_start = 12,
                width='100%', 
                height='100%',
                control_scale = True)
    cvs_group = folium.FeatureGroup(name='CVS Stores',show = False,control = True)
    wal_group = folium.FeatureGroup(name = ' Walgreen Stores',show = False,control = True)
    overlap_group = folium.FeatureGroup(name = ' CVS and Walgreen',show = True,control = True)
    cvs_wal = repo.henryhcy_jshen97_leochans_wangyp.wal_wal_cvs.find_one()
    
    overlap = []
    for key,value in cvs_wal.items():
     
        if key != "_id":    
            if value['cvs'][1] == 0.0:
                location = repo['henryhcy_jshen97_leochans_wangyp.walgreen'].find_one({"place_id":key})['geometry']['location']
                overlap.append([location['lat'],location['lng']])
                



    heat = []
    cvs_only = []
    walgreen_only = []


    for item in repo.henryhcy_jshen97_leochans_wangyp.cvsWalgreen.find():
        
        if item['name'] == 'CVS':
            color = 'lightred'
            folium.Marker([item['location']["lat"], item['location']["lng"]], 
                    tooltip='CVS',
                    icon=folium.Icon(color=color,icon='shopping-cart')).add_to(cvs_group)
            cvs_only.append([item['location']["lat"], item['location']["lng"]])
        else:
            color = 'lightgreen'
            folium.Marker([item['location']["lat"], item['location']["lng"]], 
                tooltip='walgreen',
                icon=folium.Icon(color=color,icon='shopping-cart')).add_to(wal_group)
            walgreen_only.append([item['location']["lat"], item['location']["lng"]])
        heat.append([item['location']["lat"], item['location']["lng"]])
        
    for item in overlap:
        folium.Marker(item, 
                popup=folium.Popup("cvs & walgreen"),
                tooltip="cvs & walgreen",
                icon=folium.Icon(color="pink",icon='shopping-cart')).add_to(overlap_group)
    cvs_group.add_to(m)
    wal_group.add_to(m)
    overlap_group.add_to(m)   
     
    
    m.add_children(plugins.HeatMap(heat, radius=30,name = 'heatmap',show = False))

    mc = plugins.MarkerCluster(name = 'CVS_Wal_Clustering',show = False)
    cvs_cluster = plugins.MarkerCluster(name = 'CVS_Clustering',show = False)
    wal_cluster = plugins.MarkerCluster(name = 'Wal_Clustering',show = False)

    #creating a Marker for each point in df_sample. Each point will get a popup with their zip
    for row in heat:
        mc.add_child(folium.Marker(location=row))
    for row in cvs_only:
        cvs_cluster.add_child(folium.Marker(location=row))
    for row in walgreen_only:
        wal_cluster.add_child(folium.Marker(location=row))
    
 
    m.add_child(mc)
    m.add_child(cvs_cluster)
    m.add_child(wal_cluster)
    folium.LayerControl().add_to(m)



    m.save("Flaskapp/templates/heatafter.html")

if __name__ == "__main__":
    Mapping()