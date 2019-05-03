# Project 3

## Introduction
### Data sets:
1.Property

2.Center

3.CenterPool

4.PoliceStation

5.School


### Narrative

Boston is a beatiful city, many people live here. We find that the properties in different area have totally different value and we want to find out the factor that influences the value of properties.

Firstly, we combine the Property data with all Center data, CenterPool data, PoliceStation data and School data by joining on the 'ZIPCODE' attribute, which is implemented in join_by_ZIP.py. Besides, we also calculate the average value of properties and the number of all centers, centerpools, policestations and schools of each different ZIPCODE zone by doing some aggregation operations for this data sets in join_by_ZIP.py. So the codes in join_by_ZIP.py generates a new collection which named 'ZIPCounter' and represents the data after clustering. 

Based on the clustered collection, we solve the constraint satisfaction problem in the 'optimization.py'by finding out the zipzone that has at least one center, one centerPool, one policeStation and one school, then the optimization problem is about among the zipzone just found, pointing out the zipzone which has the lowest average property value and largest number of public places, that is minimize the result of(average value in one zone)/(sum of all numbers of center, centerpool, policestation and school in that zone).

Also based on the 'ZIPCounter' collection, we calcuate the correlation coefficient of average property value and the number of center, average property value and the number of centerPool, average property value and the number of policeStation, average property value and the number of school, this algorithm in 'correlation.py' gives us the result telling whether the average property value and the number of different public places are related.According to our statistical results, they are almost not correlated.
