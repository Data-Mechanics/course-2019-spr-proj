import dml
import prov.model
import datetime
import uuid
from sklearn.cluster import KMeans
from sklearn.cluster import MiniBatchKMeans
import matplotlib.pyplot as plt
from scipy.stats.stats import pearsonr
import seaborn as sns
import time
import json
from gmplot import gmplot


class KMEANS(dml.Algorithm):

    contributor = 'hxjia_jiahaozh'
    reads = ['hxjia_jiahaozh.id_month_price_score_lat_long']
    writes = ['']

    @staticmethod
    def execute(trial=False):

        def project(R, p):
            return [p(t) for t in R]

        def select(R, s):
            return [t for t in R if s(t)]


        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hxjia_jiahaozh', 'hxjia_jiahaozh')

        collection_data = repo.hxjia_jiahaozh.id_month_price_score_lat_long
        information = collection_data.find({})
        all_data = []
        for data in information:
            all_data.append(data)


        all_data = select(all_data, lambda t: t['number_of_reviews'] > 10 )
        price_score = project(all_data, lambda t: [t['price'], t['review_score']])
        #print(len(price_score))
        #print(price_score)
        price_list = [i[0] for i in price_score]
        #print(price_list)
        # plt.title('Price Distribution')
        # plt.xlabel('Price')
        # plt.ylabel('Number')
        # sns.distplot(price_list)
        # plt.show()
        score_list = [i[1] for i in price_score]
        #print(score_list)
        # sns.distplot(score_list)
        # plt.title('Review Score Distribution')
        # plt.xlabel('Review Score')
        # plt.ylabel('Number')
        # plt.show()
        Pearsonr_price_score = pearsonr(price_list, score_list)
        print('The correlation coefficient between price and review score is: ', Pearsonr_price_score[0])
        review_number_list = project(all_data, lambda t: t['number_of_reviews'])
        Pearsonr_price_reviewnumber = pearsonr(price_list, review_number_list)
        print('The correlation coefficient between price and the number of reviews is: ', Pearsonr_price_reviewnumber[0])

    # We choose the number of clusters ranging from 1 to 10, snd then to calculate Within-Cluster-Sum-of-Squares(WCSS).
        wcss_list = []
        for k in range(1, 10):
            #t0 = time.time()
            kmeans = MiniBatchKMeans(init='k-means++', n_clusters=k, batch_size=100, random_state=0).fit(price_score)
            #km_batch = time.time() - t0
            #print("Comsuming time of fitting KMeans is:%.4fs" % km_batch)
            wcss_list.append(kmeans.inertia_)
            #kmeans.cluster_centers_
        #print('WCSS of MiniBatchKMeans is:', wcss_list)
        klist = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        plt.plot(klist, wcss_list)
        plt.title('K value vs WCSS')
        plt.xlabel('The Number of Clusters (K value)')
        plt.ylabel('Within-Clusters-Sum-of-Squares')
        #plt.show()


        kms = MiniBatchKMeans(init='k-means++', n_clusters=4, batch_size=100, random_state=0)
        y = kms.fit_predict(price_score)
        plt.title('Kmeans Result')
        plt.xlabel('Price')
        plt.ylabel('Review Score')
        count0, count1, count2, count3 = 0, 0, 0, 0
        sum0, sum1, sum2, sum3 = 0, 0, 0, 0
        lat0, lat1, lat2, lat3 = [], [], [], []
        long0, long1, long2, long3 = [], [], [], []
        for i in range(0, len(y)):
            if y[i] == 0:
                plt.plot(price_score[i][0], price_score[i][1], "*r")
                sum0 += price_score[i][0]
                lat0.append(all_data[i]['latitude'])
                long0.append(all_data[i]['longitude'])
                count0 += 1
            elif y[i] == 1:
                plt.plot(price_score[i][0], price_score[i][1], "sy")
                sum1 += price_score[i][0]
                lat1.append(all_data[i]['latitude'])
                long1.append(all_data[i]['longitude'])
                count1 += 1
            elif y[i] == 2:
                plt.plot(price_score[i][0], price_score[i][1], "pb")
                sum2 += price_score[i][0]
                lat2.append(all_data[i]['latitude'])
                long2.append(all_data[i]['longitude'])
                count2 += 1
            elif y[i] == 3:
                plt.plot(price_score[i][0], price_score[i][1], ">g")
                sum3 += price_score[i][0]
                lat3.append(all_data[i]['latitude'])
                long3.append(all_data[i]['longitude'])
                count3 += 1
        center0_x = sum0 / count0
        center1_x = sum1 / count1
        center2_x = sum2 / count2
        center3_x = sum3 / count3
        #print('center_0 is:', center0_x)
        #print('center_1 is:', center1_x)
        #print('center_2 is:', center2_x)
        #print('center_3 is:', center3_x)
        centers = [center0_x, center1_x, center2_x, center3_x]
        centerscopy = centers.copy()
        centers.sort()
        firstindex = centerscopy.index(centers[0])
        secondindex = centerscopy.index(centers[1])
        thirdindex = centerscopy.index(centers[2])
        fourthindex = centerscopy.index(centers[3])
        lat_all = [lat0, lat1, lat2, lat3]
        long_all = [long0, long1, long2, long3]

        # Boston area
        gmap = gmplot.GoogleMapPlotter(42.361145, -71.057083, 13)

        gmap.apikey = "Your Google API Key"

        #gmap.scatter(lat0+lat1+lat2+lat3, long0+long1+long2+long3, 'FF0000', size=30, marker=False)
        gmap.scatter(lat_all[firstindex], long_all[firstindex], '#FF0000', size=30, marker=False)
        gmap.scatter(lat_all[secondindex], long_all[secondindex], '#B8860B', size=30, marker=False)
        gmap.scatter(lat_all[thirdindex], long_all[thirdindex], '#0000FF', size=30, marker=False)
        gmap.scatter(lat_all[fourthindex], long_all[fourthindex], '#556B2F', size=30, marker=False)
        gmap.heatmap(lat0+lat1+lat2+lat3, long0+long1+long2+long3)
        # Show clustered google map with HTML format.
        gmap.draw("gmap.html")
        #plt.show()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hxjia_jiahaozh', 'hxjia_jiahaozh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/hxjia_jiahaozh/')

        this_script = doc.agent('alg:hxjia_jiahaozh#kmeans',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:calendar',
                              {'prov:label': 'id_month_price_score_lat_long, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})

        transformation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transformation, this_script)
        doc.usage(transformation, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Transformation',
                   'ont:Query': '?type=cid_month_price_score_lat_long&$select=price, review_score, number_of_reviews'
                   }
                  )
        KmeansOptimization = doc.entity('dat:hxjia_jiahaozh#fourclusters',
                          {prov.model.PROV_LABEL: 'KmeansOptimization', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(KmeansOptimization, this_script)
        doc.wasGeneratedBy(KmeansOptimization, transformation, endTime)
        doc.wasDerivedFrom(KmeansOptimization, resource, transformation, transformation, transformation)

        repo.logout()

        return doc


# if __name__ == "__main__":
#     KMEANS.execute()
#     doc = KMEANS.provenance()
#     print(doc.get_provn())
#     print(json.dumps(json.loads(doc.serialize()), indent=4))


