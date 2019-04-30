from scipy.cluster.vq import kmeans
import dml
from shapely.geometry import Polygon

def geojson_to_polygon(geom):
    """

    :return: list of shapely polygons corresponding to the geojson object
    """
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
    return polys

def compute_weight(dist_score, dist_mean, dist_stdev, health_score, health_mean, health_stdev, weight):
    dist_z_score = ((dist_score - dist_mean) / dist_stdev)  * (weight/100)
    #print("dist", dist_z_score)
    health_z_score = ((health_score - health_mean) / health_stdev) * (1-(weight/100))
    #print("health", health_z_score)
    average_z_score = (dist_z_score + health_z_score)

    if average_z_score > 1:
        print("average", average_z_score)
        return 100
    elif average_z_score > .5:
        return 20
    else:
        return 2



def compute_kmeans(neighborhood, num_means, passed_weight):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('gasparde_ljmcgann_tlux', 'gasparde_ljmcgann_tlux')
    parcels = repo['gasparde_ljmcgann_tlux' + ".ParcelsCombined"]
    neighborhood_parcels = list(parcels.find({"Neighborhood": neighborhood}))
    stats = repo['gasparde_ljmcgann_tlux' + ".Statistics"]
    dist_mean = float(stats.find_one({"Neighborhood": neighborhood, "variable": "distance_score", "statistic": "mean"})["value"])
    dist_stdev = float(stats.find_one({"Neighborhood": neighborhood, "variable": "distance_score", "statistic": "std_dev"})["value"])
    health_mean = float(stats.find_one({"Neighborhood": neighborhood,"variable": "health_score", "statistic": "mean"})["value"])
    health_stdev = float(stats.find_one({"Neighborhood": neighborhood, "variable": "health_score", "statistic": "std_dev"})["value"])

    kmean = []

    for i in range(100):
        shape = geojson_to_polygon(neighborhood_parcels[i]["geometry"])[0]
        # out of order, want [latitude, longitude]
        coords = [shape.centroid.coords[0][1], shape.centroid.coords[0][0]]
        weight = compute_weight(neighborhood_parcels[i]["distance_score"], dist_mean, dist_stdev,
                                neighborhood_parcels[i]["health_score"], health_mean, health_stdev, passed_weight)


        for _ in range(weight):
            kmean.append([coords[0], coords[1]])
    g = kmeans(kmean, num_means)
    output = kmeans(kmean, num_means)[0].tolist()
    return output

#print(compute_kmeans("Allston", 10))