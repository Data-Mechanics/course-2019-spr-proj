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

        ################################################################################################################
        ################################################################################################################
        ################################################################################################################
        ################################################################################################################

        url = 'http://datamechanics.io/data/gasparde_ljmcgann_tlux/boston_census_track.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        print(r[0])
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

        ################################################################################################################
        ################################################################################################################
        ################################################################################################################
        ################################################################################################################

        url = 'https://chronicdata.cdc.gov/resource/47z2-4wuh.json?placename=Boston'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        boston_cdc = []
        for data in r:
            for tract in list_of_tracks:

                if data["tractfips"] == tract["Census Tract"]:
                    d = {"obesity": data["obesity_crudeprev"], "low_phys": data["lpa_crudeprev"],
                         "asthma": data["casthma_crudeprev"]}
                    combined = {**tract, **d}

                    boston_cdc.append(combined)
                    break

        print(boston_cdc)

        ################################################################################################################
        ################################################################################################################
        ################################################################################################################
        ################################################################################################################

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

        ################################################################################################################
        ################################################################################################################
        ################################################################################################################
        ################################################################################################################

        list_of_answer = {}
        total = len(list_of_tracks)
        success = 0
        for neighborhood in list_of_neighborhoods:
            tracks = []

            for track in list_of_tracks:
                for poly_neighborhood in neighborhood['Shape']:

                    for poly_track in track['Shape']:

                        if poly_neighborhood.contains(poly_track.centroid):
                            success += 1
                            tracks.append(track)

                        list_of_answer[neighborhood['neighborhood']] = (tracks, len(tracks))

        pprint.pprint(list_of_answer, indent=4)
        print(len(list_of_answer))
        print("{}  out of {} census tracks where put in neighborhoods which is {:.2}% !".format(success, total,
                                                                                                success / total))

        ################################################################################################################
        ################################################################################################################
        ################################################################################################################
        ################################################################################################################

    #    _  __           __  __ ______          _   _  _____
    #   | |/ /          |  \/  |  ____|   /\   | \ | |/ ____|
    #   | ' /   ______  | \  / | |__     /  \  |  \| | (___
    #   |  <   |______| | |\/| |  __|   / /\ \ | . ` |\___ \
    #   | . \           | |  | | |____ / ____ \| |\  |____) |
    #   |_|\_\          |_|  |_|______/_/    \_\_| \_|_____/
    #
    #

    def union(R, S):
        return R + S

    def difference(R, S):
        return [t for t in R if t not in S]

    def intersect(R, S):
        return [t for t in R if t in S]

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]

    def product(R, S):
        return [(t, u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    def dist(p, q):
        (x1, y1) = p
        (x2, y2) = q
        return (x1 - x2) ** 2 + (y1 - y2) ** 2

    def plus(args):
        p = [0, 0]
        for (x, y) in args:
            p[0] += x
            p[1] += y
        return tuple(p)

    def scale(p, c):
        (x, y) = p
        return (x / c, y / c)

    M = [(13, 1), (2, 12)]
    P = [(1, 2), (4, 5), (1, 3), (10, 12), (13, 14), (13, 9), (11, 11)]

    OLD = []
    while OLD != M:
        OLD = M

        MPD = [(m, p, dist(m, p)) for (m, p) in product(M, P)]
        PDs = [(p, dist(m, p)) for (m, p, d) in MPD]
        PD = aggregate(PDs, min)
        MP = [(m, p) for ((m, p, d), (p2, d2)) in product(MPD, PD) if p == p2 and d == d2]
        MT = aggregate(MP, plus)

        M1 = [(m, 1) for (m, _) in MP]
        MC = aggregate(M1, sum)

        M = [scale(t, c) for ((m, t), (m2, c)) in product(MT, MC) if m == m2]
        print(sorted(M))


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
