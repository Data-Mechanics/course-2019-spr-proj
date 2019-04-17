import dml
import prov.model
import datetime
import uuid
from scipy import stats
import numpy as np


class alert_weather_stats(dml.Algorithm):
    contributor = 'kgarber'
    reads = ['kgarber.alert_weather_daily']
    writes = ['kgarber.alert_weather_stats']

    @staticmethod
    def execute(trial = False):
        print("Starting alert_weather_stats algorithm.")
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgarber', 'kgarber')
        repo.dropCollection("alert_weather_stats")
        repo.createCollection("alert_weather_stats")

        pts = [pt for pt in repo['kgarber.alert_weather_daily'].find()]

        if trial:
            pts = np.random.choice(pts, size=100)

        temps = [pt["tempAvg"] for pt in pts]
        alert_count = [pt["totalAlerts"] for pt in pts]

        corr, p_val = stats.pearsonr(temps, alert_count)
        best_fit = np.polyfit(temps, alert_count, 1)
        best_fit = list(best_fit)
        print("Temperature and MBTA Alerts:")
        print("\tcorrelation", corr)
        print("\tp-value", p_val)
        print("\tbest-fit", best_fit)

        repo['kgarber.alert_weather_stats'].insert({
            "correlation": corr,
            "p-value": p_val,
            "best-fit": best_fit
        })

        # indicate that the collection is complete
        repo['kgarber.alert_weather_stats'].metadata({'complete':True})
        
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finished alert_weather_stats algorithm.")
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
        alert_weather_stats_ent = doc.entity(
            'dat:kgarber#rides_weather_stats',
            {
                prov.model.PROV_LABEL:'Alert and Weather Stats',
                prov.model.PROV_TYPE:'ont:DataSet'
            })
        alert_weather_daily = doc.entity(
            'dat:kgarber#ride_weather_aggregate',
            {
                prov.model.PROV_LABEL: 'Alert and Weather Daily',
                prov.model.PROV_TYPE: 'ont:DataSet'
            })
        gen_aggregate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(gen_aggregate, this_script)
        doc.usage(gen_aggregate, alert_weather_stats_ent, startTime, None,
                {prov.model.PROV_TYPE:'ont:Correlation'})
        doc.wasAttributedTo(alert_weather_stats_ent, this_script)
        doc.wasGeneratedBy(alert_weather_stats_ent, gen_aggregate, endTime)
        doc.wasDerivedFrom(alert_weather_stats_ent, alert_weather_daily, 
            gen_aggregate, gen_aggregate, gen_aggregate)
        return doc

# alert_weather_stats.execute()
# alert_weather_stats.execute(True)
# print(alert_weather_stats.provenance().get_provn())
