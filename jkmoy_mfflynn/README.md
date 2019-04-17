## Group members:
Justin Moy (jkmoy@bu.edu)

Mary Flynn (mfflynn@bu.edu)

# Project #1

The datasets that we chose to retrieve are ones that are based on emergency services in Boston. We pulled things like car accidents, fire reports, and crime reports. We are not sure yet of the one problem that we wish to solve using our data and its transformations, but we were interesting in seeing the statistics from which these incidents happened. For our data on fire and the # of fire departments, we totaled the number of fire calls (whether it was real or accident) by town and divided by the number of fire departments in that town to see average incidents per department. For our car accident data, we totaled the amount of occurences for every pair of mode and location to see how the pairs were more dangerous than others. For our final transformation, using the crime dataset, we counted the occurences per crime based on the day of the week, in order to see if crimes occured on some days more than the others. A breakdown of the files we created are broken down below:
### Retrievals 
* accidents.py - Retrieves the dataset for Vision Zero's fatal and non-fatal crash records
* crime.py - Retrieves the dataset of crime incident reports in Boston from August 2015 to present
* fire.py - Retrieves the dataset of fire incident reports in Boston
* fireDepartments.py - Retrieves the dataset of information about the different fire departments in every town in Boston

### Transformations
* averagePerDepartment.py - counts the number of fire incidents per town and finds the average amount per department by dividing by the number of departments in the corresponding town
* dotw.py - Count the distribution of crime reports by day to see if crime occurred on some days more than others
* modeLocationCount.py - Count the different car accidents and street type pairs to see if a certain mode of transportation (vehicle, bike, etc) was more dangerous/prone to accidents on a certain type of street (one-ways, intersections, etc).

# Project #2
The purpose of our project is to analyze the overall safety of Boston and each of its towns. To try to achieve this, we grabbed datasets concerning emergency services in Boston, as documented in project #1. For this project, we mainly focused on the 9-1-1 crime reports and road accidents. We had to exclude using fire incident reports because the dataset lacked any type of fields that helped pinpoint a location (latitude and longitude). For our constraint satisfaction, we chose to run k-means clustering, where k = 23, the number of official towns in Boston. With those 23 centroids, we plotted each crime incident and road accident based on their latitude and longitude and found the centroid each point was closest to. We did this 2 separate times, one for crime and one for road accidents, they were not done together. Once this was done, we derived statistics from each of the clusters as our statistical analysis. For every cluster for crime, we calculated the number of violent crimes and calculated the standard deviation and average across all clusters so that we could see the distribution of violent crimes throughout Boston. Similarly, we did the same thing for road accidents, but we just counted accidents in general, so we could see how traffic accidents were different in every town/cluster. At the end of this, we had a general idea of how safe or unsafe Boston could be as a whole. Clustering allowed us to separate out different parts of the city and see if it were safe or unsafe, relative to the other clusters. 

To achieve this in programming, we utilized the scikit-learn library for python3:
```bash
from sklearn.cluster import KMeans
```

## Running 
To run our code, clone the data and cd into the top-level directory (course-2019-spr-proj) and run:
```bash
python3 execute.py jkmoy_mfflynn
```
