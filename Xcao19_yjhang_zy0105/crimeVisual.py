import os
import folium
import dml


class crimeVisual():
    # print(folium.__version__)
    contributor = 'Jinghang_Yuan'
    reads = ['Jinghang_Yuan.crimes']
    writes = []

    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('Jinghang_Yuan', 'Jinghang_Yuan')

    R = list(repo['Jinghang_Yuan.crime'].find({},{'_id':0,'Lat':1,'Long': 1}))
    R = R[0:200:1]

    # lon, lat = 42.26260773, -71.12118637

    # m = folium.Map(location=[42.26260773, -71.12118637])

    m = folium.Map(
        location=[R[1]['Lat'], R[1]['Long']],
        tiles='OpenStreetMap',
        zoom_start=13
    )

    tooltip = 'Click me!'

    for s in R:
        # print(s['Lat'])
        # print(s['Long'])
        if(s['Lat']!=None and s['Long']!=None):
            folium.Marker([s['Lat'], s['Long']], popup='<i>Mt. Hood Meadows</i>', tooltip=tooltip).add_to(m)

    m.save(os.path.join('2.html'))

    # print(m)
crimeVisual

