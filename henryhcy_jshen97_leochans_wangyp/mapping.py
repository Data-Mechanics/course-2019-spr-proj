import folium
from folium import plugins
import dml

reads = ['henryhcy_jshen97_leochans_wangyp.neighborhoods']
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')

def Mapping():
   

    m = folium.Map(location = [42.3601,-71.0589],zoom_start = 12,
                width='75%', 
                height='75%',
                control_scale = True)
    tooltip = 'click here for more info'
    folium.Marker([42.3846071,-71.1197947],popup = '<strong>Location One</trong>',tooltip=tooltip).add_to(m)
    cvs_wal = repo.henryhcy_jshen97_leochans_wangyp.wal_wal_cvs.find_one()
    
    overlap = []
    for key,value in cvs_wal.items():
     
        if key != "_id":    
            if value['cvs'][1] == 0.0:
                location = repo['henryhcy_jshen97_leochans_wangyp.walgreen'].find_one({"place_id":key})['geometry']['location']
                overlap.append([location['lat'],location['lng']])
    print(overlap)
                



    heat = []

    for item in repo.henryhcy_jshen97_leochans_wangyp.cvsWalgreen.find():
        
        if item['name'] == 'CVS':
            color = 'lightred'
        else:
            color = 'lightgreen'
        heat.append([item['location']["lat"], item['location']["lng"]])
        folium.Marker([item['location']["lat"], item['location']["lng"]], 
                popup=folium.Popup(item['name']),
                tooltip=tooltip,
                icon=folium.Icon(color=color,icon='shopping-cart')).add_to(m)
    for item in overlap:
        folium.Marker(item, 
                popup=folium.Popup("cvs & walgreen"),
                tooltip=tooltip,
                icon=folium.Icon(color="pink",icon='shopping-cart')).add_to(m)
        
    m.add_children(plugins.HeatMap(heat, radius=30))

    


    folium.LayerControl().add_to(m)




    m.save("Flaskapp/templates/heatafter.html")

if __name__ == "__main__":
    Mapping()