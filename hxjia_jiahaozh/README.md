
# CS504 Data-Mechanics Project 1

## Authors
Haoxuan Jia(hxjia@bu.edu)
Jiahao Zhang(jiahaozh@bu.edu)

## Datasets
### Boston Airbnb Calendar
A csv file containing the price on a cetain day between year 2016 and 2017
<br />http://datamechanics.io/data/hxjia_jiahaozh/Calendar.csv
### Boston Airbnb Listings
A csv file containing detailed information about a house
<br />http://datamechanics.io/data/hxjia_jiahaozh/Listings.csv
### Boston Airbnb Reviews
A csv file containing reviews given by users to a specific house
<br />http://data.insideairbnb.com/united-states/ma/boston/2019-01-17/data/reviews.csv.gz
### Boston Landmarks
A csv file containing landmarks in different neighborhoods of Boston
<br />http://bostonopendata-boston.opendata.arcgis.com/datasets/7a7aca614ad740e99b060e0ee787a228_3.csv
### US Holidays
A csv file containing date and holidays from 1966 to 2011
<br />http://datamechanics.io/data/hxjia_jiahaozh/US_Holidays.csv

## Purpose
With the datasets above, we can combine them to answer 3 interesting questions:
<br />1.	What is the impact of holidays on the prices of Boston Airbnb houses? Is there any pattern that the average price is highest or lowest on some holiday?
<br />2.	What is the impact of Boston landmarks on the number of houses and on the average prices of houses?
<br />3.	What is the relationship between the prices of houses and traveler's reviews.

## Data Transformation
### Price_and_Comments
Generated from Boston Airbnb Listings and Boston Airbnb Reviews
<br />Boston Airbnb Listings: Project to get data in a form of (listing_id, price, review_score)
<br />Boston Airbnb Reviews: Project to get data in a form of (listing_id, comments);
                             Aggregate to get a combination of comments for each listing_id
<br />Combination: product + Project to get the comments and review score for each listing_id, in the form of (listing_id, price, comments, review_score)
### Price_on_Holidays
Generated from Boston Airbnb Calendar and US_Holidays
<br />Boston Airbnb Calendar: Select the data with valid price;
                        Project to get data in a form of (date, price);
                        Aggregate to get the mean price for each date
<br />US_Holidays: Select holidays in the range of Boston Airbnb Calendar's dates
<br />Combination: Product + Project to get the mean price for each date and whether the date is a holiday, in the form of (date, avg_price, holiday)
### Prices_Landmarks_Listings
Generated from Boston Airbnb Listings and Boston Landmarks
<br />Boston Airbnb Listing: Select, Project  Aggregate to get (neighbourhood, the number of houses in that neighbourhood), Select, Project and Aggregate to get (neighbourhood, the mean price of houses in that neighbourhood).
<br />Boston Landmarks: Select, Project and Aggregate to get (neighbourhood, the number of landmarks in that neighbourhood)
<br />Combiantion: project to get  (neighbourhood, the number of landmarks,  the number of houses, the mean prices of houses in that neighbourhood)
## Tools and Libraries
Pandas
<br />dml
<br />prov
<br />protoql
<br />json
<br />uuid
<br />urllib.request

# CS504 Data-Mechanics Project 2
## Narrative
The idea of this project was to find the best way to categorize the airbnb houses based on reviews and prices. Figure 1 and Figure 2 show the basic housing distribution on Google map and the corresponding heatmap to show the housing density in areas. In the project, we first calculated the correlation coefficient between prices and review scores to find if they two are independent enough to do k-means. Then we calculated Within-Cluster-Sum-of-Squares(WCSS) under different K values to choose best K for clustering. Finally, we plotted each cluster according to latitudes and longtitudes on googlemap using different colors.
<img src="https://github.com/jiahaozh/course-2019-spr-proj/blob/master/hxjia_jiahaozh/Experimental_Results/Housing_Scatter_Map.png" />
<p align="center">Figure 1</p>
<img src="https://github.com/jiahaozh/course-2019-spr-proj/blob/master/hxjia_jiahaozh/Experimental_Results/Housing_Heap_Map.png" />
<p align="center">Figure 2</p>

## Data Transformation
This part was implemented in id_month_price_score_lat_long.py
<br />Generated from Boston Airbnb Listings and Boston Airbnb Calendar 
<br />Boston Airbnb Calendar: Select, Project and Aggregate to get (id, mean price of all dates of the year) 
<br />Boston Airbnb Listing: Select, Project to get (id, review scores, the number of reviews, longitude, latitude) 
<br />Combination: Select, Project to get (id, mean price, review scores, the number of reviews, longitude, latitude)
## Statistical Analysis
This part was implemented in KMEANS.py
<img src="https://github.com/jiahaozh/course-2019-spr-proj/blob/master/hxjia_jiahaozh/Experimental_Results/Price_Distribution.png" />
<p align="center">Figure 3</p>
<img src="https://github.com/jiahaozh/course-2019-spr-proj/blob/master/hxjia_jiahaozh/Experimental_Results/Review_Score_Distribution.png" />
<p align="center">Figure 4</p>
<br />We calculated the correlation coefficient between prices and review scores and the correlation coefficient between prices and the numbers of reviews. The results are as follows.
<br />Prices and Review Scores: 0.1172
<br />Prices and the numbers of Reviews: -0.1565 
<br />The results show that the two pairs are both independent enough to do Kmeans, in the project we used price and review score in the next step.

## Optimization
This part was also implemented in KMEANS.py
<br />We calculated the Within-Cluster-Sum-of-Squares(WCSS) for k-means, finding the best k value is 4, so we did a 4-means and plotted each cluster onto googlemap to reveal the relationship between each cluster and their geolocation.

<img src="https://github.com/jiahaozh/course-2019-spr-proj/blob/master/hxjia_jiahaozh/Experimental_Results/K_WCSS.png" />

<img src="https://github.com/jiahaozh/course-2019-spr-proj/blob/master/hxjia_jiahaozh/Experimental_Results/Kmeans_Result.png" />

<img src="https://github.com/jiahaozh/course-2019-spr-proj/blob/master/hxjia_jiahaozh/Experimental_Results/Classified_Map.png" />



## Improvements
We did 2 improvements on K-Means.
### 1. K-Means++
Although given enough time KMeans will always converge, this may lead to a local minimum. It is highly dependent on the 
initialization of the centoids. To overcome this issue, we used k-means++ initialization scheme. Just set init = 'k-
means++' parameter in Sklearn. This helped initializing the centroids to be (generally) distant from each other. And it helped 
speed up converging.

### 2. Mini Batch K-Means
The Mini Batch K-Means is a variant of K-Meanns, which uses mini-batches to shorten computation time. In contrast to other algorithms that reduce the convergence time of k-Means, mini-batch k-Means produces results that are generally only slightly worse than the standard algorithm.
As a result, the computation was shortened to its half while the WCSSs differed slightly.


