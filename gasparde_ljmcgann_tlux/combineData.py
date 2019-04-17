import datetime
import uuid
import dml
import prov.model
from math import *
from rtree import index
from shapely.geometry import Polygon
from tqdm import tqdm


class combineData(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = [contributor + ".CensusTractShape", contributor + ".CensusTractHealth",
             contributor + ".Neighborhoods", contributor + ".ParcelAssessments",
             contributor + ".ParcelGeo", contributor + ".OpenSpaces"]
    writes = [contributor + ".ParcelsCombined"]

    @staticmethod
    def haversine(point1, point2):
        """
        computes correct distance between two points given
        :param point1:
        :param point2:
        :return: the geometrical distance in kilometers
        """
        lon1 = point1[1]
        lon2 = point2[1]
        lat1 = point1[0]
        lat2 = point2[0]

        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1

        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(min(1, sqrt(a)))
        r = 6371  # Radius of earth in kilometers. Use 3956 for miles

        return c * r

    @staticmethod
    def geo_distance(point, poly2):
        min_distance = 1000
        # to find geo distance between a point and a polygon just
        # iterate through each point that makes up the polygon and
        # return the minimum distance from the point to one of the
        # points in the polygon
        for other_point in list(poly2.exterior.coords):
            min_distance = min(min_distance, combineData.haversine((point.x, point.y), other_point))
        return min_distance

    @staticmethod
    def create_neighborhood_dict(neighborhoods):
        """
        returns dictionary with keys of neighborhoods whose value is an
        empty list
        :return: dictionary
        """
        val = {}
        for neighborhood in neighborhoods:
            print(neighborhood)
            val[neighborhood['properties']['Name']] = []
        return val

    @staticmethod
    def geojson_to_polygon(geom):
        """
        take geojson geometry and turns into a shapely object
        :return: shapely object
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

    @staticmethod
    def distance_scores(data):
        parcel_index = index.Index()
        for i in range(len(data)):
            parcel_bounds = combineData.geojson_to_polygon(data[i]["geometry"])[0].bounds
            parcel_index.insert(i, parcel_bounds)

        for i in tqdm(range(len(data))):
            score = 0
            new_park = combineData.geojson_to_polygon(data[i]["geometry"])[0]
            for other_parcel in [j for j in parcel_index.nearest(new_park.bounds, 50)]:
                score += data[other_parcel]["min_distance_km"]

            data[i]["distance_score"] = score

        return data

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(combineData.contributor, combineData.contributor)

        ####################################################################

        # putting opens spaces into their respective neighborhoods
        # if an open spaces is in or overlaps a neighborhoods we put this
        # into the neighborhood, thus allows for an open space to be in
        # multiple neighborhoods
        open_spaces = list(repo[combineData.contributor + ".OpenSpaces"].find())

        neighborhoods = list(repo[combineData.contributor + ".Neighborhoods"].find({"properties.Name":"Allston"})) if trial \
            else list(repo[combineData.contributor + ".Neighborhoods"].find())

        open_space_index = index.Index()

        # some open spaces are multipolygons, so we are turning the list of open spaces into
        # a list of single polygons where we break up the multipolygons so that we can insert
        # into a rtree
        open_spaces_flattened = []
        for open_space in open_spaces:
            geom = open_space["geometry"]
            if geom['type'] == 'Polygon':
                shape = []
                coords = geom['coordinates']
                for i in coords[0]:
                    shape.append((i[0], i[1]))
                open_spaces_flattened.append([shape, open_space["properties"]["OBJECTID"]])
            if geom['type'] == 'MultiPolygon':
                coords = geom['coordinates']
                for i in coords:
                    shape = []
                    for j in i:
                        for k in j:
                            # need to change list type to tuple so that shapely can read it
                            shape.append((k[0], k[1]))
                    open_spaces_flattened.append([shape, open_space["properties"]["OBJECTID"]])

        for i in range(len(open_spaces_flattened)):
            open_space_shapely = Polygon(open_spaces_flattened[i][0])
            open_space_index.insert(i, open_space_shapely.bounds)
        ###############################################################

        # put parcel shapes together with its parcel assessment
        parcel_geo = list(repo[combineData.contributor + ".ParcelGeo"].find())[:10000] if trial \
            else list(repo[combineData.contributor + ".ParcelGeo"].find())
        parcel_assessments = repo[combineData.contributor + ".ParcelAssessments"]

        parcel_shape_assessment = []
        # PID LONG
        print("Combining Parcels Shape with their Assessments:")
        for i in tqdm(range(len(parcel_geo))):
            PID = parcel_geo[i]["properties"]["PID_LONG"]
            assessment = parcel_assessments.find_one({"_id": PID})
            parcel_shape = parcel_geo[i]["geometry"]
            if assessment is not None:
                parcel_shape_assessment.append({**assessment, **{"geometry": parcel_shape}})

        ids = set()
        unique_parc_shape_assess = []
        for i in range(len(parcel_shape_assessment)):
            if parcel_shape_assessment[i]["_id"] not in ids:
                unique_parc_shape_assess.append(parcel_shape_assessment[i])
                ids.add(parcel_shape_assessment[i]["_id"])
            else:
                print(parcel_shape_assessment[i]["_id"])

        # print(len(parcel_shape_assessment))
        # print(parcel_shape_assessment)

        ###############################################################

        # put census tract shapes together with its health statistics

        census_tract_health = repo[combineData.contributor + ".CensusTractHealth"]
        census_tract_shape = list(repo[combineData.contributor + ".CensusTractShape"].find())

        c_t_health_shape = []
        for i in range(len(census_tract_shape)):
            tract = census_tract_shape[i]["Census Tract"]
            health = census_tract_health.find_one({"_id": tract})
            shape = {"geometry": {"type": census_tract_shape[i]["type"],
                                  "coordinates": census_tract_shape[i]["coordinates"]}}
            if health is not None:
                c_t_health_shape.append({**health, **shape})

        ###############################################################

        # add which tract the parcel is in as well as the tracts health score

        # rtree for tracts
        tract_index = index.Index()
        for i in range(len(c_t_health_shape)):
            tract_shapely = combineData.geojson_to_polygon(c_t_health_shape[i]["geometry"])
            tract_index.insert(i, tract_shapely[0].bounds)

        print("Combining Parcel with Tracts")
        parcels_with_health = []
        for i in tqdm(range(len(unique_parc_shape_assess))):
            found = False
            parcel_shapely = combineData.geojson_to_polygon(unique_parc_shape_assess[i]["geometry"])[0]
            for ti in [j for j in tract_index.nearest(parcel_shapely.bounds, 5)]:
                tract_shapely = combineData.geojson_to_polygon(c_t_health_shape[ti]["geometry"])
                for shape in tract_shapely:
                    if shape.contains(parcel_shapely):
                        tract_data = {"asthma": c_t_health_shape[ti]["asthma"],
                                      "low_phys": c_t_health_shape[ti]["low_phys"],
                                      "obesity": c_t_health_shape[ti]["obesity"],
                                      "Census Tract": c_t_health_shape[ti]["_id"]}
                        data = {**unique_parc_shape_assess[i], **tract_data}
                        parcels_with_health.append(data)
                        found = True
                        break
                if found:
                    break
        # print(parcel_shape_assessment)
        # print(len(parcel_shape_assessment))

        ###############################################################

        # add parcels to neighborhoods
        parcels_by_neighborhood = combineData.create_neighborhood_dict(neighborhoods)
        print("Adding Parcels to Neighborhoods")
        for i in tqdm(range(len(parcels_with_health))):
            found = False
            for neighborhood in neighborhoods:
                neighborhood_shapely = combineData.geojson_to_polygon(neighborhood["geometry"])
                parcel_shapely = combineData.geojson_to_polygon(parcels_with_health[i]["geometry"])[0]
                for shape in neighborhood_shapely:
                    if shape.contains(parcel_shapely):
                        parcels_by_neighborhood[neighborhood["properties"]["Name"]].append(parcels_with_health[i])
                        found = True
                        break
                if found:
                    break

        ##############################################################

        # add distance to closest park and improvement scores to each parcel
        # in each neighborhood

        for neighborhood in list(parcels_by_neighborhood.keys()):
            print("Computing Min Distances for " + neighborhood + ":")
            for i in tqdm(range(len(parcels_by_neighborhood[neighborhood]))):
                min_distance = 100
                min_open_space = None
                open_space_id = None
                data = parcels_by_neighborhood[neighborhood][i]
                parcel_shapely = combineData.geojson_to_polygon(data["geometry"])[0]
                for op_i in [j for j in open_space_index.nearest(parcel_shapely.bounds, 5)]:
                    op_s_shapely = Polygon(open_spaces_flattened[op_i][0])
                    distance = parcel_shapely.distance(op_s_shapely)
                    if distance < min_distance:
                        min_distance = distance
                        min_open_space = op_s_shapely
                        open_space_id = open_spaces_flattened[op_i][1]
                if min_open_space is not None:
                    min_distance_km = combineData.geo_distance(parcel_shapely.centroid, min_open_space)
                else:
                    min_distance_km = 100
                parcels_by_neighborhood[neighborhood][i]["min_distance"] = min_distance
                parcels_by_neighborhood[neighborhood][i]["min_distance_km"] = min_distance_km
                parcels_by_neighborhood[neighborhood][i]["nearest_open_space"] = open_space_id
                parcels_by_neighborhood[neighborhood][i]["Neighborhood"] = neighborhood

        repo.dropCollection(combineData.contributor + ".ParcelsCombined")
        repo.createCollection(combineData.contributor + ".ParcelsCombined")
        for neighborhood in list(parcels_by_neighborhood.keys()):
            print("Computing Scores for", neighborhood)
            data = parcels_by_neighborhood[neighborhood]
            parcels_by_neighborhood[neighborhood] = combineData.distance_scores(data)
            repo[combineData.contributor + ".ParcelsCombined"].insert_many(parcels_by_neighborhood[neighborhood])
        repo[combineData.contributor + ".ParcelsCombined"].metadata({'complete': True})

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        this_script = doc.agent('alg:gasparde_ljmcgann_tlux#combineData',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'],
                                 'ont:Extension': 'py'})

        CensusTractShape = doc.entity('dat:gasparde_ljmcgann_tlux#CensusTractShape',
                                      {prov.model.PROV_LABEL: 'Boston Census Tract Shape',
                                       prov.model.PROV_TYPE: 'ont:DataSet'})
        CensusTractHealth = doc.entity('dat:gasparde_ljmcgann_tlux#CensusTractHealth',
                                       {prov.model.PROV_LABEL: 'Boston Census Tract Health Statistics',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        Neighborhoods = doc.entity('dat:gasparde_ljmcgann_tlux#Neighborhoods',
                                   {prov.model.PROV_LABEL: 'Shape of Boston Neighborhoods',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        OpenSpaces = doc.entity('dat:gasparde_ljmcgann_tlux#OpenSpaces',
                                {prov.model.PROV_LABEL: 'Open Spaces in Boston that can be Parks',
                                 prov.model.PROV_TYPE: 'ont:DataSet'})
        ParcelGeo = doc.entity('dat:gasparde_ljmcgann_tlux#ParcelGeo',
                               {prov.model.PROV_LABEL: 'The Shape of the Parcels', prov.model.PROV_TYPE: 'ont:DataSet'})
        ParcelAssessments = doc.entity('dat:gasparde_ljmcgann_tlux#ParcelAssessments',
                                       {prov.model.PROV_LABEL: 'Assessment Value of Parcels',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        getParcelsCombined = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(getParcelsCombined, this_script)
        doc.usage(getParcelsCombined, CensusTractShape, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(getParcelsCombined, CensusTractHealth, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(getParcelsCombined, Neighborhoods, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(getParcelsCombined, OpenSpaces, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(getParcelsCombined, ParcelGeo, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(getParcelsCombined, ParcelAssessments, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        ParcelsCombined = doc.entity('dat:gasparde_ljmcgann_tlux#ParcelCombined',
                                     {prov.model.PROV_LABEL: 'Final Dataset Produced for Optimization and Analysis',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(ParcelsCombined, this_script)

        doc.wasGeneratedBy(ParcelsCombined, getParcelsCombined, endTime)

        doc.wasDerivedFrom(ParcelsCombined, CensusTractHealth, getParcelsCombined, getParcelsCombined,
                           getParcelsCombined)
        doc.wasDerivedFrom(ParcelsCombined, CensusTractShape, getParcelsCombined, getParcelsCombined,
                           getParcelsCombined)
        doc.wasDerivedFrom(ParcelsCombined, Neighborhoods, getParcelsCombined, getParcelsCombined,
                           getParcelsCombined)
        doc.wasDerivedFrom(ParcelsCombined, OpenSpaces, getParcelsCombined, getParcelsCombined,
                           getParcelsCombined)
        doc.wasDerivedFrom(ParcelsCombined, ParcelGeo, getParcelsCombined, getParcelsCombined,
                           getParcelsCombined)
        doc.wasDerivedFrom(ParcelsCombined, ParcelAssessments, getParcelsCombined, getParcelsCombined,
                           getParcelsCombined)
        return doc
