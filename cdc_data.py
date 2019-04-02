import urllib.request
import json
from shapely.geometry import Polygon
import pprint
# import dml
# import pymongo


class health:  # (dml.Algorithm):

    @staticmethod
    def execute(trial=False):
        # client = pymongo.MongoClient()
        # repo = client.repo
        # repo.authenticate('gasparde_ljmcgann_tlux', 'gasparde_ljmcgann_tlux')

        url = 'http://datamechanics.io/data/gasparde_ljmcgann_tlux/boston_census_track.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        # print(r[0])
        list_of_tracks = []
        census_tracks = r  # repo.gasparde_ljmcgann.health
        for geom in census_tracks:
            polys = []
            if geom['type'] == 'Polygon':
                shape = []
                coords = geom['coordinates']
                for i in coords[0]:
                    shape.append((i[0], i[1]))
                polys.append(Polygon(shape))
            if geom['type'] == 'MultiPolygon':
                coords = geom['coordinates']
                for i in coords:
                    shape = []
                    for j in i:
                        for k in j:
                            # need to change list type to tuple so that shapely can read it
                            shape.append((k[0], k[1]))
                    poly = Polygon(shape)
                    polys.append(poly)

            list_of_tracks.append({"Shape": polys, "Census Tract": geom["Census Tract"]})
        # print(list_of_tracks)
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        # print(r['features'])
        neighborhoods = r['features']
        list_of_neighborhoods = []
        for neighborhood in neighborhoods:
            geom = neighborhood['geometry']
            polys = []
            if geom['type'] == 'Polygon':
                shape = []
                coords = geom['coordinates']
                for i in coords[0]:
                    shape.append((i[0], i[1]))
                polys.append(Polygon(shape))
            if geom['type'] == 'MultiPolygon':
                coords = geom['coordinates']
                for i in coords:
                    shape = []
                    for j in i:
                        for k in j:
                            # need to change list type to tuple so that shapely can read it
                            shape.append((k[0], k[1]))
                    poly = Polygon(shape)
                    polys.append(poly)

            list_of_neighborhoods.append({'neighborhood': neighborhood['properties']['Name'], "Shape": polys})
        # print(list_of_neighborhoods)

        list_of_answer = {}
        total = len(list_of_tracks)
        success = 0
        for neighborhood in list_of_neighborhoods:
            tracks = []

            for track in list_of_tracks:
                for poly_neighborhood in neighborhood['Shape']:

                    for poly_track in track['Shape']:

                        if poly_neighborhood.contains(poly_track.centroid):
                            success = success + 1
                            tracks.append(track)

                        list_of_answer[neighborhood['neighborhood']] = (tracks, len(tracks))

        pprint.pprint(list_of_answer, indent=4)



        print("{}  out of {} census tracks where put in neighborhoods which is {:.2}% !".format(success, total,
                                                                                                success / total))


# for data in health.find({"CityName": "Boston"}):
#     GeoLocation = Point(data["GeoLocation"][0], data["GeoLocation"][1])
#
#     neighBorhood = [neighborhood["Name"] for neighborhood in repo.gasparde_ljmcgann.neighborhoods.find() if
#                     shape(neighborhood["geometry"]).contains(GeoLocation)]
#     neighBorhood = neighBorhood[0]
#     print(neighBorhood)
#     r = {"_id": data["_id"], "Category": data["Category"], "Measure": data["Measure"],
#          "ShortQuestion": data["Short_Question_Text"], "Neighborhood": neighBorhood, "GeoLocation": GeoLocation}
#     break
#
# repo.logout()


if __name__ == '__main__':
    health.execute()
