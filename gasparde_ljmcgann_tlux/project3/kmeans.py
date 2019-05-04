import dml
from scipy.cluster.vq import kmeans
from shapely.geometry import Polygon
from rtree import index


def compute_weight(dist_score, dist_mean, dist_stdev, health_score, health_mean, health_stdev, weight):
    dist_z_score = ((dist_score - dist_mean) / dist_stdev) * (1 - (weight / 100))
    # print("dist", dist_z_score)
    health_z_score = ((health_score - health_mean) / health_stdev) * ((weight / 100))
    # print("health", health_z_score)
    average_z_score = (dist_z_score + health_z_score)

    if average_z_score > 1.5:
        return 100
    elif average_z_score > 1:
        return 10
    else:
        return 1

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


def compute_kmeans(neighborhood, num_means, passed_weight):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('gasparde_ljmcgann_tlux', 'gasparde_ljmcgann_tlux')
    parcels = repo['gasparde_ljmcgann_tlux' + ".ParcelsCombined"]
    neighborhood_parcels = list(parcels.find({"Neighborhood": neighborhood}))
    stats = repo['gasparde_ljmcgann_tlux' + ".Statistics"]
    dist_mean = float(
        stats.find_one({"Neighborhood": neighborhood, "variable": "distance_score", "statistic": "mean"})["value"])
    dist_stdev = float(
        stats.find_one({"Neighborhood": neighborhood, "variable": "distance_score", "statistic": "std_dev"})[
            "value"])
    health_mean = float(
        stats.find_one({"Neighborhood": neighborhood, "variable": "health_score", "statistic": "mean"})["value"])
    health_stdev = float(
        stats.find_one({"Neighborhood": neighborhood, "variable": "health_score", "statistic": "std_dev"})["value"])

    kmean = []
    parcel_index = index.Index()
    for i in range(len(neighborhood_parcels)):
        shape = geojson_to_polygon(neighborhood_parcels[i]["geometry"])[0]
        parcel_index.insert(i, shape.bounds)
        # out of order, want [latitude, longitude]
        coords = [shape.centroid.coords[0][1], shape.centroid.coords[0][0]]
        weight = compute_weight(neighborhood_parcels[i]["distance_score"], dist_mean, dist_stdev,
                                         neighborhood_parcels[i]["health_score"], health_mean, health_stdev,
                                         passed_weight)

        for _ in range(weight):
            kmean.append([coords[0], coords[1]])
    output = kmeans(kmean, num_means)[0].tolist()
    dict = {"kmeans": str(output)}
    dict["Avg_Land_Val"] = []
    dict["Dist_To_Park"] = []
    dict["Avg_Health"] = []
    for mean in output:
        point = (mean[1], mean[0], mean[1], mean[0])

        bounds = [i for i in parcel_index.nearest(point, 5)]
        avg_val = 0
        dist_to_park = 0
        health_score = 0
        count = 0
        # take only five observations in case there are more due to ties
        for ij in bounds[:5]:
            if neighborhood_parcels[ij]["AV_TOTAL"] is not None and neighborhood_parcels[ij]["LAND_SF"] is not None:
                avg_val += round(
                    float(neighborhood_parcels[ij]["AV_TOTAL"]) / float(neighborhood_parcels[ij]["LAND_SF"]), 2)
                dist_to_park += float(neighborhood_parcels[ij]["min_distance_km"])
                health_score += float(neighborhood_parcels[ij]["health_score"])
                count += 1
        dict["Avg_Land_Val"].append(round(avg_val / count, 2))
        dict["Dist_To_Park"].append(round(dist_to_park / count, 2))
        dict["Avg_Health"].append(round(health_score / count, 2))

    return dict