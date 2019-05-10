Project Two
------------
After browsing through Boston’s Open Data Portal, we noticed that police compensation was a lot higher than we expected for officers in the city of Boston. As a result, we became interested in the allocation and distribution of police resources within the city. Towards a solution to that problem, we’ve looked at both: where crimes are centered among Boston’s 12 police districts, and which demographics might correlate with crime incidents across districts. With this information, we can determine which point in a given police district minimizes the mean distance to where crimes are occuring in that district and ways we might allocate police resources in those districts to better address criminal activity.

Something else that we decided to look at was if there were any ways that we could predict the number of incidents within a district. We investigated three main factors taken from Boston census data: per capita income, education level (bachelor’s degree or higher attained), and percentage of white non-hispanics in a neighborhood. For each of these factors we calculated the IQR to see if there were any outliers at the neighborhood level and also at the district level. Then, we calculated the correlation coefficient for each of the factors for each district against the number of crime incidents per district to see if there was any connection. Finally, we found the districts with the highest and lowest number of incidents and stored the details of those specific districts in our database for later reference.

The libraries that are needed for this part of the project are numpy, pandas, sklearn, scipy, geopandas.

--------------------

Project One
------------

Data Set One
---------------
One data set we pulled was a zipped shape file from Harvard Dataverse that contained information about the police districts within Boston. In order to better use the data, we then converted it into a GeoJSON format. In this way, the data will store nicely within MongoDB and will be easier to query. As for the significance of this data, while the data set itself is not interesting, it can be combined with some of our other data sets. One data sets that we could combine are the crime statistics for each district with this data set. This would allow us to better visualize the crime per district. We could also use the race and ethinicity or general neighborhood statistics for us to store more information about each district.

Data Set Two
---------------
Another data set is the race and ethincity data set gathered from census data. Due to the nature of the data (it was presented in a pdf), it was better for us to store the data in a different format and have it on the datamechanics.io site. What is great about this data is that not only does it provide valuable insight into the race and ethnicities that make up each neighborhood of Boston, it can easily be combined with other data sets. For example, we combined it with a data set that matched each Boston neighborhood to a police district. As a result, we are able to come up with a better picture of what the demographics are for each district. This was done through a map reduce function.

Data Set Three
---------------
We also pulled the salary data for all employees employed by the city of Boston (teachers, etc.) from data.boston.gov. These data include total compensation, base salary, as well as overtime and various extra payments. From the data, we filtered for employees of the Boston Police Department. Then, we pulled out relevant subsections of salary for each officer and aggregated them to compute the total amount paid for each category using MapReduce (total, overtime, detail, etc.). This transformation gives us better insight into the amounts spent towards police salaries by the City of Boston. The city spends nearly 400 million on police compensation, and there are some officers who have made upwards of $100,000 in overtime or detail work (more than their base salaries) which we find intriguing.  


Data Set Four
---------------
We also pulled the BPD Field Interrogation and Observation records from data.boston.gov. These include traffic stops, car searches, wellness checks, etc. and include officer names, dates, and incident descriptions. We plan to use this dataset with our police salary information to potentially look at which officers are mentioned in the most FIO entries, and by what types. This could be useful given we have compensation information for each officer and so we may discover a trend of some kind there. We can also look at the locations of each incident and aggregate them by police district to potentially find trends there. It may also be interesting to see if certain police spend time in certain areas, potentially helping us see where the city's resources are being used.

Data Set Five
---------------
Finally, we have pulled the Boston Police Department's Crime Incident Reports, which document crimes reported and documented by the BPD. Similarly to the FIO records mentioned above, we can look at how different officers were involved in dealing with various crimes and crime trends based on location. These data are notably different from the FIO records, though, in that these are include all activity responded to by BPD and the FIO records only include occurences initiated by police and which only sometimes result in discovered criminal activity. With these two datasets we may obtain a more complete view of police activity than with either alone. Thus, we could potentially begin to figure out what relationships exist between compensation (the expenditure of city resources) and police engagement (the expenditure of BPD resources). 

