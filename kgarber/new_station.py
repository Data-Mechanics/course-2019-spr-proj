import dml
import prov.model
import datetime
import uuid
from scipy import cluster
import numpy as np
import z3


# this is an INCORRECT way to do distance,
# you should use the Haversine formula
# it's a bit complicated
def basic_distance(lat1, lon1, lat2, lon2):
    return ((lat1-lat2)**2 + (lon1-lon2)**2)**0.5


# optimization works with LINEAR constraints,
# calculating distance uses squares... so instead use this
def linear_distance(lat1, lon1, lat2, lon2):
    return z3_abs(lat1 - lat2) + z3_abs(lon1 - lon2)


# a z3 friendly way of implementing absolute value
def z3_abs(num):
    return z3.If(num >= 0, num, -num)


class new_station(dml.Algorithm):
    contributor = 'kgarber'
    reads = ['kgarber.bluebikes.stations']
    writes = ['kgarber.bluebikes.new_station']

    @staticmethod
    def execute(trial = False):
        print("Starting new_station algorithm.")
        startTime = datetime.datetime.now()
        trial_size = 25
        num_clusters = 50
        max_dist = 0.04
        min_num_close_stations = 2
        # num_results = 5

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgarber', 'kgarber')
        repo.dropCollection("bluebikes.new_station")
        repo.createCollection("bluebikes.new_station")

        # read from mongodb
        print("Reading stations from DB.")
        stations = [st for st in repo['kgarber.bluebikes.stations'].find()]
        if trial:
            print("Running in trial mode.")
            stations = np.random.choice(stations, size=trial_size)
        # [[longitude, latitude], ...]
        stations = [
            [
                st["location"]["geometry"]["coordinates"][0],
                st["location"]["geometry"]["coordinates"][1]
            ] for st in stations]
        # filter to make sure coordinates are correct (we have some bad data)
        stations = [st for st in stations if -75 < st[0] < -70 and 40 < st[1] < 45]
        if trial:
            clusters = stations
        else:
            # get 100 clusters of stations,
            # any more than that is too hard for the optimizer.
            # also, clustering helps us remove weight from dense station areas.
            print("Running k-means.")
            clusters, _ = cluster.vq.kmeans(stations, num_clusters)
        num_pts = len(clusters)

        # objective variables
        z3.set_option(precision=6)
        o_lon, o_lat = z3.Reals('o_lon o_lat')
        # distances from new station to all other points
        distances = [linear_distance(o_lon, o_lat, cl[0], cl[1]) for cl in clusters]
        dist_sum = sum(distances)
        # restrict how far the station gets placed from a few other stations
        # prevents bad network branching
        num_close_enough = sum([z3.If(d < max_dist, 1, 0) for d in distances])
        has_close_enough = num_close_enough > min_num_close_stations

        # set up the optimization solver
        opt = z3.Optimize()
        # make sure we don't place the station too far away from a few other stations
        opt.add(has_close_enough)
        # keep the station out of the atlantic ocean
        opt.add(o_lon < -70.032)
        # maximize the distance to other stations
        print("Running optimization...")
        maximized = opt.maximize(dist_sum)

        if (opt.check() == z3.sat):
            m = opt.model()
            print("Longitude:", m[o_lon].as_decimal(5))
            print("Latitude:", m[o_lat].as_decimal(5))
            repo['kgarber.bluebikes.new_station'].insert({
                "longitude": m[o_lon].numerator_as_long() / m[o_lon].denominator_as_long(),
                "latitude": m[o_lat].numerator_as_long() / m[o_lat].denominator_as_long()
            })
            # indicate that the collection is complete
            repo['kgarber.bluebikes.new_station'].metadata({'complete':True})
        else:
            print("No result for optimization...")
        
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finished new_station algorithm.")
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # our data mechanics class namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')

        this_script = doc.agent(
            'alg:kgarber#alert_weather_stats', 
            {
                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 
                'ont:Extension':'py'
            })
        bluebikes_stations = doc.entity(
            'dat:kgarber#bluebikes_stations', 
            {
                prov.model.PROV_LABEL:'Bluebikes Stations', 
                prov.model.PROV_TYPE:'ont:DataSet'
            })
        new_station_ent = doc.entity(
            'dat:kgarber#new_station',
            {
                prov.model.PROV_LABEL:'New Station',
                prov.model.PROV_TYPE:'ont:DataSet'
            })
        gen_station = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(gen_station, this_script)
        doc.usage(gen_station, new_station_ent, startTime, None,
                {prov.model.PROV_TYPE:'ont:Optimization'})
        doc.wasAttributedTo(new_station_ent, this_script)
        doc.wasGeneratedBy(new_station_ent, gen_station, endTime)
        doc.wasDerivedFrom(new_station_ent, bluebikes_stations, 
            gen_station, gen_station, gen_station)
        return doc

# new_station.execute()
# new_station.execute(True)
# print(new_station.provenance().get_provn())
