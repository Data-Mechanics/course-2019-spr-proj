# CS504 Project: Proper Boston Tour (Updated for Project#2)
## Contributors:
- Soojee Kim
- Soohyeok Lee


### Project Goal:
Our goal is to determine best travel experiences for incoming tourists within Greater Boston Area. Having such an immense area, people may not have their best experiences in their limited time of travel and we wanted to suggest specific areas based on various datasets for the best possible experience.

### Data sources Used:
- Analyze Boston (*data.boston.gov*)
- Boston Maps Open Data (*bostonopendata-boston.opendata.arcgis.com*)
- Massachusetts Department of Transportation (*geo-massdot.opendata.arcgis.com*)

### Datasets Used:
1. Boston Neighborhoods (*get_neighborhoods.py*)
https://data.boston.gov/dataset/boston-neighborhoods
2. Crime rate (*get_crimeData.py*)  
https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system
3. Boston Landmarks Commission (BLC) Historic Districts (*get_landmarks.py*)   
http://bostonopendata-boston.opendata.arcgis.com/datasets/547a3ccb7ab443ceaaba62eef6694e74_4
4. MBTA Bus Stops (*get_busStops.py*)  
https://geo-massdot.opendata.arcgis.com/datasets/2c00111621954fa08ff44283364bba70_0
5. MBTA Station stops (*get_trainStations.py*)  
https://geo-massdot.opendata.arcgis.com/datasets/train-stations?geometry=-73.51%2C41.878%2C-69.555%2C42.59

### Project Description:
We currently put together the few datasets listed above and transformed the acquired datasets to see which neighborhoods within the Greater Boston Area  
- has greater number of landmarks to see
- has better system of public transportation
- has low crime rates.

Although we have different datasets of polygons and points, our current project model is heavily dependent on geolocation datsets. We are currently researching on possible datasets to incorporate into our project to further develop user experience.

## *Project#2 justification*
We needed a way to rate the neighborhoods somehow based on the coordinate datas we have collected. The problem is that we have each neighborhood's landmark coordinates, public tranportation coordiantes and crime coordiantes (where crime occured) but we do not have a way to compare a neighborhood to another neighborhood.

We decided to use K-means to find cluster of coordinates with positive values (landmark coordinates and public transporation coordinates). The K-means would give us a K number of coordiantes where the data is clustered at. This would not let us compare the neighborhoods but based on where each coordinate lies, we may be able to take this information to rate the neighborhoods. We could possibly decide to give the coordinates to the center of the clusters found by the K-mean algorithm for the users to possibly create a better experience.

Then we found the average distance of all features of a neighborhood from its averaged coordinate of its features (if the wording here is confusing, I have described what each file does below; please scroll down to where file name includes *stat*). I would call this algorithm to be somewhat of an scuffed insight to K-means. Based on the resulting averaged distances to averaged coordinates of neighborhood's features, we are able to rank the neighborhoods in a manner (I will refer to the averaged distance value as stats value). The stats value would tell us which town would be better to travel to based on how clustered the features are in each town. Basically lower value means landmarks and transportations are packed tightly together within the neighborhood. This is not comparable directly to the K-means but the stats algorithm gives us a different insight within each town's data of coordinates.

Also, we created different variations for the K-means and stats algorithm for user experience in that
- transportation coordinates are included or excluded: considering user may use public transportation or just simply ride uber(or drive their own car)
- crime coordinates are included or excluded: considering safety may not be a issue to the user

### Transformation:  
#### *transform_landmark.py* (file name-changed and updated from *landmarkRate.py*):
- Pulls dataset of polygons from *get_neighborhoods.py*
- Pulls dataset of polygons from *get_landmarks.py*
- Polygon datset of landmarks is averaged into points
- Now that we have points, checks where the crime points are marked within the neighborhood polygons.

#### *transform_crime.py* (file name-changed and updated from *crimeRate.py*):
- Pulls dataset of polygons from *get_neighborhoods.py*
- Pulls dataset of points from *get_crimeData.py*
- Checks  where the crime occurred within which polygons of neighborhoods.

#### *transform_transportation.py* (file name-changed and updated from *transportation.py*):
- Pulls dataset of polygons from *get_neighborhoods.py*
- Pulls dataset of points from *get_trainStations.py*
- Pulls dataset of points from *get_busStops.py*
- Merges two dataset of points of bus and train
- Checks where the bus stops and trainstations are within which polygons of neighborhoods.

### non-trivial constraint satisfaction or optimization technique:
- There are different variation for the k-means for the different visualization we are preparing for project#3
#### *k-means_landmark.py*:
- K-means algorithm for finding clusters of landmarks
- locates K coordinates that are centers of the found clusters 
![](kmeans_landmark.png)
#### *k-means_landmark_crime.py*:
- K-means algorithm for finding clusters of landmarks
- landmark coordinates close to crime coordinates are removed
- locates K coordinates that are centers of the found clusters
![](kmeans_landmark_crime.png)
#### *k-means_landmark_transportation.py*:
- K-means algorithm for finding clusters of landmarks and transporations (bus & train)
- locates K coordinates that are centers of the found clusters
![](kmeans_landmark_transportation.png)
#### *k-means_landmark_transportation_crime.py*:
- K-means algorithm for finding clusters of landmarks and trasnportations (bus & train) where
- landmark or transporation coordinates close to crime coordinates are removed 
- locates K coordinates that are centers of the found clusters 
![](kmeans_landmark_transportation_crime.png)

### statistical analysis or inference algorithm:
- There are different variation for the stats alg for the different visualization we are preparing for project#3
#### *stat_landmark.py*:
- finds the averaging center point of landmarks based on each neighborhood's landmark coordinates
- then finds the average distance to each landmark to the found coordinate
#### *stat_landmark_crime.py*:
- landmark coordinates near crime coordinates are removed
- finds the averaging center point of landmarks based on each neighborhood's landmark coordinates
- then finds the average distance to each landmark to the found coordinate
#### *stat_landmark_transportation.py*:
- finds the averaging center point of landmarks and transportations based on each neighborhood's landmark coordinates and transportation coordinates
- then finds the average distance to each landmarks and transporations to the found coordinates
#### *stat_landmark_transportation_crime.py*:
- landmark coordinates near crime coordinates are removed
- finds the averaging center point of landmarks and transportations based on each neighborhood's landmark coordinates and transportation coordinates
- then finds the average distance to each landmarks and transporations to the found coordinates

### Execution Script for Provenance.html:
To execute all the algorithms for the project in an order that respects their explicitly specified data flow dependencies, run the following from the root directory:
```
python3 execute.py soohyeok_soojee
```

### Note:
If you have any suggestions to improve tourist experience or possible dataset to incorporate onto our project please leave a comment on github or send any of us an e-mail
- soohyeok@bu.edu
- soojee@bu.edu
