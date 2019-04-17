# Project 2
In this project, we aim to try to find the best places to add
new parks in each of the neighborhoods in Boston based on various
health statistics as well as access to open spaces. 

###Data Sets
1. [Boston Maps Open Data](http://bostonopendata-boston.opendata.arcgis.com/)
    - [Open spaces geospatial data in Boston](http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson)
    - [Boston neighborhoods geospatial data](http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson)
    - [Parcels Geojson](http://bostonopendata-boston.opendata.arcgis.com/datasets/b7739e6673104c048f5e2f28bb9b2281_0.geojson)
    - [Parcels Assessments](https://data.boston.gov/datastore/odata3.0/fd351943-c2c6-4630-992d-3f895360febd)
2. [Center for Disease Control and Prevention](https://chronicdata.cdc.gov/)
    - [Health survey data in Boston](https://chronicdata.cdc.gov/resource/csmm-fdhi.json?cityname=Boston)
3. [Data Mechanics Portal](http://datamechanics.io/)
    - [Census Tracks Geojson](http://datamechanics.io/data/gasparde_ljmcgann_tlux/boston_census_track.json)

###Description
In **getData.py**, we make get requests to each of the six different datasets
to retrieve the datasets. The only unique data retrieval case was for the parcel
assessment data, where we had to make repeated queries to the api as it didn't
allow for more than 32,000 of the over 170,000 rows to be pulled in one request.
After retrieving the datasets, we combine all of with **combineData.py**. 
The data is combined in the following order: 
1. First, we put into a dictionary where each key corresponds to each neighborhood in Boston the list of open spaces
that overlaps (is partially or fully contained) said neighborhood using Shapely.
2 Combine parcel geojson shape with certain assessment value like square footage and total land value by 
their PID (parcel id).
3. In the CDC health survey data, the surveys are asked in each census tract in Boston. Therefore, we merge by
Census Tract Numbers the shape and health statistics of each tract. The certain health statistics that we 
are focusing in on are obesity, asthma, and low physical activity as these logically might be related someones
access or proximity to open spaces.
4. Then, using Shapely, we determine for each parcel which Census Tract it lies in, and assigning to it the
Census Tract's health statistics.
5. Since most Census Tracts don't lie perfectly in a neighborhood, we instead then put each parcels into each neighborhood,
store into a dictionary where each key is a neighborhood and the value is the list of parcels in each neighborhood.
6. Then, for each parcel we compute the distance to the closest open space, stored as min_distance_km, computing using the haversine function to translate
latitude and longitudinal coordinate differences to kilometers. Also, we compute a distance score for each neighborhood,
which consists of taking its 10 closest neighboring parcels and summing their min_distance_km. 

Note: We implemented a few r-trees indexes to speed up merging and computation based on geojson coordinates. Also, some neighborhoods
are dropped like the Leather District for reasons such as too small of a neighborhood, lack of parcels, and lack of open spaces.

### Optimization
For the optimization, which is implemented in **optimize.py**, we run a weighted k-means algorithm (in our case k = 5) to find the optimal
locations for new parks in each neighborhood. To implement weighted k-means, we implement a metric on each parcel to 
compute its weight, and add that many points to the set of point we will run k-means on. In this implementation we 
computed two metrics. The first metric was distance_scores, and we did this based on the number of standard deviations
a parcels distance_score was from the mean. The second metric was based on the three health statistics, which were the 
percentage of people who suffered from obesity, asthma, and low physical activity. We took an average of these values
and depend on this average was assigned a weight accordingly. The important thing to note is that the higher the average,
the more unhealthy an area was, and so this corresponded to a higher weight. Our goal of this optimization was to compute the 
best five potential locations in each neighborhood where we favored spots that weren't close to any open spaces
relative to other parcels in the area or areas with lower levels of health.

### Statistics
For the statistics portion of this project, we first computed the mean and standard deviations
of each of the three health statistics as well as the distance scores for each neighborhood. We
also computed the correlation coefficients between distance scores and each fo the three health 
statistics. We did this for each neighborhood as well. The goal was to see what the correlation was
between an area's proximity/access to an open space and the relative healthiness of that area. If we
find high correlation between any of these statistics this would give some evidence that it might 
be justified to favor unhealthy areas as possible locations for new parks/open spaces.

### Trial Mode
We also have implemented a trial mode. Here, instead of running through each neighborhood, we focus on
parcels in Boston. We also limit the number of parcels we are looking at so that the algorithm will run faster.
The statistics and k-means will also only be computed for Allston.