# CS504 Project: Revere Crash Data History

Using Revere traffic collision data from 2002-2018, and combining it with other data sources, we want to find the factors that contribute the most to causing traffic collisions. The factors we want to consider are:
- Weather (daily weather information from NOAA)
- Pedestrians (predicted by population data)
- New Development (shown by City data of recent and anticipated development)
- Speed limits (from the Google Roads API and Waze)
- Flooding and extreme weather (because Revere is on the coast)
- Natural ambient Light (from databases that show time of day and season and natural light at those hours)
- Other Similar Cities (from MassDOT), compare to Lynn, Chelsea, Malden, Medford, Everett, Quincy

We want to use a difference in differences technique to model the factors, including heat-mapping historical accident data and how it has changed over time..

Then, using the Revere traffic collision data, we want to create a predictive heat map to show where traffic collisions are most likely to happen in Revere, given the factors above. We want to do predictive analysis based on where development is happening to determine where future traffic accident hotspots may occur in the future. 


## Group Member
- Zehui Jiang
- Runqi Tian
- Xin He
- Hongyao Fei

### Datasets
- Revere Crash Data 01-19
- Town Varied Crash Data
- Fatality vs Town
- Revere Units Added
- Unit Annual Difference

These datasets contains information about every recorded car crash in Revere in the last 20 years. Along with the detail of every crash like IDs, weather condition, place of crash, fatality, etc.

Source / portals of datasets:
- Revere government
- services.massdot.state.ma.us/crashportal

Datasets may come in as .pdf file, .xlsx file or .txt file. We have convert them all to .csv file or .json file and uploaded to datamechism.io for further use.

### Non-Trivial Transformations Done/New Datasets Created

- Our first major transformation was creating the crash file. We extracted all the parameters that we find relevent to the crash. In this case, we chose Light condition, Weather, Fatality and of course, ID of the crash. This projection will give us more convenience in studying the relationship between natural factors and the occurrance of the crash.

 - Another transformation was an aggregation. This started with taking annual increase in units in different location in Revere. As we ahave no access to the annaully traffic information, the increase in units stands for the traffic of each location. But in this case, we would like to first study the annual change of traffic data. So we aggregate the unit change number by year.

 - Our final transformation involved the dataset containing the crash data in several other towns near Revere. With this dataset, we want a town-oriented study and compare the statistics in Revere to that of other surrounding towns. So we first use a projection to cleanup crash Ids as there may exists two different kind of notation. Then we use another projection to put the ID together with crash date, time and most importantly, in which town.
 
 
### Supplemental dataset
We supplement a dataset of car crash spots and visualize it on google map, we will also use this dataset to do our Project2 


### Optimization Problem

#### Problem:


We have the dataset of all car-crashing spots in revere city. After using google map to visualize these spots, we found that the car-crashing spots are distributed everywhere in this city. However, we still found several spots in the car accidents heat map where car accidents are most likely to happen, some spots like traffic circle.
We believe that there are still other spots which we can not discover simply by observing the heat map of car
accidents.So we decided to build up a model about this problem, using the Optimization Techniques to solve
this problem.

![image](https://github.com/TTTheo/course-2019-spr-proj/raw/master/robinhe_rqtian_hongyf_zhjiang/heatmap.png) 

#### The problem is about:


Find n spots in revere city where car accidents are most likely to happen. In this problem, n is a variable
you can choose yourself.

 

#### Solution:


We abstract this real world problem to a mathematical model. To simplify the problem, we divide the map
into grid map and calculate the car accidents happened in each grid. Then we use a slide window to
go throw the grids and find the grid where car accidents are most likely to happen. For example n=5, then
we have C1,C2,C3,C4,C5 for 5 positions of the slide window.  

 
 
System states S = {grid in this grid map}^n  



then the constraints are:


1, the grid map is 100*100


2, the slide window slides inside the region, the stride is 1 and the window size is 3, it should also be
a square window.


3, Ci and Cj can not overlap each other, which means if the slide window overlaps, we only keep the bigger one.  



the metric is:


1, C = C1 +C2 + ... + C5, we need to find a state in S which maximize C 



Running Example:


    {'left-up': (42.43524010097626, -71.02176294730468), 'right-up': (42.43524010097626, -71.01961702927072), 'right-bottom': (42.43691010921584, -71.01961702927072), 'left-bottom': (42.43691010921584, 42.43524010097626)}
    {'left-up': (42.40684996090348, -70.99315070685178), 'right-up': (42.40684996090348, -70.99100478881782), 'right-bottom': (42.40851996914305, -70.99100478881782), 'left-bottom': (42.40851996914305, 42.40684996090348)}
    {'left-up': (42.429673406844344, -71.01961702927072), 'right-up': (42.429673406844344, -71.01747111123674), 'right-bottom': (42.431343415083916, -71.01747111123674), 'left-bottom': (42.431343415083916, 42.429673406844344)}
    {'left-up': (42.40851996914305, -71.0031649910103), 'right-up': (42.40851996914305, -71.00101907297633), 'right-bottom': (42.41018997738263, -71.00101907297633), 'left-bottom': (42.41018997738263, 42.40851996914305)}
    {'left-up': (42.42076669623327, -71.00602621505558), 'right-up': (42.42076669623327, -71.00388029702162), 'right-bottom': (42.42243670447285, -71.00388029702162), 'left-bottom': (42.42243670447285, 42.42076669623327)}


These are top 5 areas where car accidents are most likely to happen.
    
    
### Statistics:


#### Problem:


The number of injuries depends on the severity of the crash. Here in the dataset, we have columns of data about
injuries in every accident, non-fatal and fatal ones. We also have columns about number of cars involved in the
accident. We want to find out if there is any relation between them.


the problem is about:


the relation between the number of cars involved in the accident and the number of injuries.



#### Solution:


1, calculate average and standard deviation of non-fatal injuries, fatal injuries, injuries and number of cars.



2, calculate correlation coefficient between these dimensions.



Running Examples:


    'avg_non_fatal': 0.41782467236293974, 
    'avg_fatal': 0.00246297929272965, 
    'avg_injuries': 0.4202876516556694, 
    'avg_car': 1.9341077021315414, 
    'std_non_fatal': 0.7734077132371413, 
    'std_fatal': 0.054253363526365506, 
    'std_injuries': 0.7763725934315647, 
    'std_car': 0.626645342789055, 
    'cov_nonfatal_car': 0.025190077082430692, 
    'cov_fatal_car': -0.0007803303394241795, 
    'cov_injuries_car': 0.024409746743014386, 
    'corr_nonfatal_car': 0.05197556046145466, 
    'corr_fatal_car': -0.022952500641731856, 
    'corr_injuries_car': 0.050173137652113155
    
    
**The right to use these data is granted**