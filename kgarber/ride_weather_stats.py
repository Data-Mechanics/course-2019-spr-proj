import dml
import prov.model
import datetime
import uuid
from scipy import stats
import numpy as np


class ride_weather_stats(dml.Algorithm):
    contributor = 'kgarber'
    reads = ['kgarber.bluebikes.rides_and_weather']
    writes = ['kgarber.bluebikes.ride_weather_stats']

    @staticmethod
    def execute(trial = False):
        print("Starting ride_weather_aggregate algorithm.")
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgarber', 'kgarber')
        repo.dropCollection("bluebikes.ride_weather_stats")
        repo.createCollection("bluebikes.ride_weather_stats")

        pts = [pt for pt in repo['kgarber.bluebikes.rides_and_weather'].find()]

        if trial:
            pts = np.random.choice(pts, size=100)

        temps = [pt["tempAvg"] for pt in pts]
        durations = [pt["avgDuration"] for pt in pts]
        trip_count = [pt["numTrips"] for pt in pts]

        corr_1, p_val_1 = stats.pearsonr(temps, durations)
        best_fit_1 = np.polyfit(temps, durations, 1)
        best_fit_1 = list(best_fit_1)
        print("Temperature and Trip Duration:")
        print("\tcorrelation", corr_1)
        print("\tp-value", p_val_1)
        print("\tbest-fit", best_fit_1)

        corr_2, p_val_2 = stats.pearsonr(temps, trip_count)
        best_fit_2 = np.polyfit(temps, trip_count, 1)
        best_fit_2 = list(best_fit_2)
        print("Temperature and Trip Count:")
        print("\tcorrelation", corr_2)
        print("\tp-value", p_val_2)
        print("\tbest-fit", best_fit_2)

        repo['kgarber.bluebikes.ride_weather_stats'].insert({
            "duration": {
                "correlation": corr_1,
                "p-value": p_val_1,
                "best-fit": best_fit_1
            },
            "count": {
                "correlation": corr_2,
                "p-value": p_val_2,
                "best-fit": best_fit_2
            }
        })

        # indicate that the collection is complete
        repo['kgarber.bluebikes.ride_weather_stats'].metadata({'complete':True})
        
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finished ride_weather_stats algorithm.")
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # our data mechanics class namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')

        this_script = doc.agent(
            'alg:kgarber#ride_weather_stats', 
            {
                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 
                'ont:Extension':'py'
            })
        ride_weather_stats_ent = doc.entity(
            'dat:kgarber#rides_weather_stats',
            {
                prov.model.PROV_LABEL:'Rides and Weather Stats',
                prov.model.PROV_TYPE:'ont:DataSet'
            })
        ride_weather_agg = doc.entity(
            'dat:kgarber#ride_weather_aggregate',
            {
                prov.model.PROV_LABEL: 'Ride Weather Aggregate',
                prov.model.PROV_TYPE: 'ont:DataSet'
            })
        gen_aggregate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(gen_aggregate, this_script)
        doc.usage(gen_aggregate, ride_weather_stats_ent, startTime, None,
                {prov.model.PROV_TYPE:'ont:Correlation'})
        doc.wasAttributedTo(ride_weather_stats_ent, this_script)
        doc.wasGeneratedBy(ride_weather_stats_ent, gen_aggregate, endTime)
        doc.wasDerivedFrom(ride_weather_stats_ent, ride_weather_agg, 
            gen_aggregate, gen_aggregate, gen_aggregate)
        return doc

# ride_weather_stats.execute()
# ride_weather_stats.execute(True)
# print(ride_weather_stats.provenance().get_provn())
