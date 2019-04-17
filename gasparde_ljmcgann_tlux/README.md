# Project 2
In this project, we aim to try to find the best places to add
new parks in each of the neighborhoods in Boston based on various
health statistics as well as access to open spaces. 

###Data Sets
1. [Boston Maps Open Data](http://bostonopendata-boston.opendata.arcgis.com/)
    - [Open spaces geospatial data in Boston](http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson)
    - [Boston neighborhoods geospatial data](http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson)
    - [Parcels Geojson]
    - [Parcels Assessments]
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

