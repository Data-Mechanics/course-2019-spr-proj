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

## Project #3
**Introduction**
Boston is a sprawling city, growing day-to-day with new students, families and businesses. In such a relatively dense city, one thing we must all keep in mind is our safety. So just how safe is Boston and its 23 towns? This is what our goal for the project was, with safety in mind, we wanted to analyze the overall safety of the 23 neighborhoods/towns that the city of Boston recognizes. By the end of this project we hoped that anybody could use the findings of this project to be safer, whether you’re a resident, business owner, and/or homeowner. 

**Data:**
To start our project, we obtained data for three different types of incidents: accidents, crime, and fire reports from Boston. For our fire datasets, we used one from the Boston Fire Department, that included the incidents of fires, found here https://bit.ly/2Lj3VZG. We also used a dataset for data about the Fire Departments in Boston, that can be found at http://bit.ly/2Wnv7Ya. For accident reports we found data through Analyze Boston, the reports were done by Vision Zero and can be found at https://bit.ly/2XVQ7pl and at https://bit.ly/2ITW3Mq. These two data sets are for normal accidents and for fatal accidents and combined have about 20,000 data points. For crime reports, we used the same website to find data sets from the Boston police department, which can be found at https://bit.ly/2g3khXJ. This data set has about 390,000 data points. 

**Methods and Techniques:**
At first, we used the data we found to do something a little different from our final project. We found the modes of locations for some of the data and the data for the days of the week the incidents toke place. Although we did not end up using some of these results, we were able to figure out how to manipulate the data through them. We decided to then shift gears from those findings, and focused more towards separating the incidents by location. Once we realized that we wanted to focus on the locations of the reports, we found that we could no longer use our data for the fire departments and fires, seeing how they had no fields for location, like longitude and latitude. So, we continued with just the accident and the crime reports. From those datasets, we created a new dataset that had just the longitude and latitude and then the type of incident that occurred, for each report. Accidents had three types, motor vehicle (mv), pedestrian (ped), and bikes. While crime had many more incidents, for example larceny, assault, etc. After we got these data sets, we ran the exact same algorithm for the two types. Also, some of the entries had to be disregarded, as their longitude and latitude fields were null. 

From these new datasets, we utilized the k-means algorithm to break the data up into clusters. We wanted the means to follow a rough outline of the neighborhoods in Boston, so we decided to use 23 as our k, which is the number of neighborhoods Boston officially recognizes. From there, we finalized that data set by including, for each entry: the longitude and latitude, the cluster it belonged to, and the type of incident. With the clusters, we wanted to evaluate the neighborhoods individually and against the other neighborhoods using statistical analysis. To do this, we looked at each type of incident and calculated the number of times that incident occurred in each cluster. Knowing the number for each cluster, we then calculated the mean and the standard deviation of the type of incident in each cluster, across all of the clusters. We then create our final data set that had the means and standard deviations according to the type of incident. 

Using the clusters and statistical findings, we wanted to plot the points on a map on Boston showing the individual clusters, while also displaying some of the statistical findings we found. To do this we used Folium to plot the various clusters with different colored points. For crime, the number of points was too large, so we decided to use only the serious crimes. Crimes such as MBTA fare evasion, property lost, etc. 

**Web Application:**
The web application for this project, is a simple page, that displays two tables, one for accidents and one for crime, that give the mean and the standard deviation for the type of incident across all of the clusters. There is also the option to click to view either the accident or crime map produced by Folium, where the user can scroll over the clusters and see statistical data. For crime, it is the percent of serious crimes present in the cluster. For accidents, the number of each incident is given, along with the total number of incidents in that cluster. 

**Findings and Conclusion:**

**Accident Clusters:**
![alt text](https://github.com/J-Moy/course-2019-spr-proj/blob/master/jkmoy_mfflynn/visualizations/accident.png)

**Crime Clusters:**
![alt text](https://github.com/J-Moy/course-2019-spr-proj/blob/master/jkmoy_mfflynn/visualizations/crime.png )
For our accident reports, we found that there was not a big discrepancy in number of incidents from neighborhood to neighborhood, but there were a few interesting things to note. As expected, there were more accidents in the denser parts of the city and inversely less in the more spread out parts of Boston. But there were outliers to this, such as Mattapan, Dorchester, and Roxbury. These towns had similar numbers of accidents to the denser parts of Boston, even though they are relatively less dense to the other towns. For our Crime findings, most neighborhoods had the same percentage of serious crime, all being about 42%. Like accidents, there were outliers, with Mattapan having nearly 2x the amount of crime, whereas their population was only slightly higher. 

With our findings, we concluded that crime and vehicular accidents are pretty uniform throughout Boston’s towns. There is no strong indication that any town/cluster is more prone to any type of crime or reckless driving.  However, it may be helpful in the future to extend our statistical analysis and data considered, to gain a more precise outcome. It also might help to look towards the neighboring towns of Boston, such as Cambridge and Brookline.

**To run code:**
To run our code, clone the data and cd into the top-level directory (course-2019-spr-proj) and run:
```bash
python3 execute.py jkmoy_mfflynn
```

**To run app:**
cd into jkmoy_mfflynn/project3 and run:
```bash
python app.py
```
Open local host 5000 

