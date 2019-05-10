# Project 2


## Team Members:

Roberto Alcalde Diego<br/>
Darren Hoffmann-Marks<br/> 
Alyssa Gladding<br/>



## The problem:

We are working with a Spark! project, our partner provided us with the traffic accidents data for the last few years in the city of Revere and wanted us to run an analysis 	so that policies could be implemented to reduce the frequency of traffic accidents and their severity.

### How we chose to address the problem:

In order to compute a useful set of results that could help Revere implement effective policies, we wanted to look at the fluctuations of accidents in the city over the 	years.For these, we used a data set containing data from the years 2002 to 2016 that detailed every single accident on record in the city of Revere. We wanted to answer 	three main questions:

1 - How has the city evolved? i.e. how did the overall number of yearly accidents change over the years from 2002 to 2016.<br/>
2 - How has the severity of accidents evolved over time? (i.e. partitioning the accidents between Non-Injury, Non-Fatal Injury, and Fatal Injury) <br/>
3 - The most important, complex, and meaningful question was: For each predetermined area of Revere, did any of those areas have a significant change in the number of accidents recorded between consecutive  years? This will require using the k-means algorithm to cluster all the accidents over the past years around certain mean points and analyzing how the number of accidents mapped to those means change. Then, analyze these changes and use the standard deviation of the changes to detect any years where there were significant changes.<br/>

Answering question 3 will allow us to see which policies or phenomena may have significantly affected the number of accidents in a specific area of the city. For example, if 	in the area around the cluster point (x,y), we have that the number of accidents halved between the years 2003 and 2004 -- this could mean that an effective measure was 	taken by the city or the police department to reduce the occurrence of accidents and our project partner can potentially use this information to reduce accidents in other 	areas. 

Answering question 1 helps us inspect if any city wide policies have been globally successful.

Answering question 2 help us inspect if there were any changes in Revere that affected the frequency of accidents of certain severities.


## The Final Output:

5 Data Sets that answer our questions:

1 - MainStats: Answers question 1 by showing the way the number of yearly accidents each year has changed <br/>
2 - SeverityStats: Answers question 2 by showing the way the number of accidents of each severity changes from year to year <br/>
3 - SeverityPercentageOfAccidents: Shows the percentage of accidents that match each kind of severity <br/>
4 - ClusterEvolution: Shows how the the number of accidents surrounding each cluster point has changed from year to year <br/>
5 - AdvancedClusterAnalysis: Compute the mean yearly change of each cluster, the standard deviation and determine which years offered peak changes. <br/>
