# PROJECT #1 REPORT

We have 5 data sets:
1) BU-TRANSPORTATION-STUDY
2) Property Assessment
3) Boston Greenhouse Gas Emissions
4) Public Works Active Work Zones
5) Traffic Count Locations
We combine these in three ways, documented below.

Goal: Combine house price with people who take the bus  
Justification: As we have been tasked to find reasons why so many students are not taking the bus, we want to see if there is a correlation between house prices and whether or not a student takes the bus. It may be possible that many families cannot afford to take a bus, or even that families that are well off may not see the value in paying for a bus ride when the parents themselves can take their children to school. This is shown in two different transformations: one for those who take the bus, and one for those who don't.
  
Goal: Show the ratio of students that take the bus to students who don't.
Justification: This will give us a good idea of the percentage of students that aren't using the bus, and give us a good number to point at in an argument for the benefits of better bus routes.


# PROJECT #2 REPORT

  Our goal for this project is to increase the number of students taking buses to and from school. We aim to decrease the walking distance between the bus stop and the student’s houses, as we assume this is a major deterrent to bus ridership. 

  To address this problem we decided to use k-means and minimize the distance that students have to walk. We used data retrieved from project 1--student addresses and bus stop locations--and divided it up by school. We included both students that already take the bus and students that don’t. 

  Using the current bus stops as our centroids and the students’ addresses as the points, we ran k-means to find the optimal positions for each stop, based on the distance to each student’s house. 

  For our first statistical analysis, we calculated the average distance between students walking to old bus stops vs. the average distance it would be if new stops were implemented. Our implementation of k-means algorithm yielded smaller distances traveled on average by the students.  
  
  For our second statistical analysis we calculated the correlation between household value and if they take the bus or not. This will tell us the impact income has on the likeliness of a student taking the bus. Using proportion agreement (accuracy) we had an accuracy of ~80%. We used binary variables to represent whether or not a student takes the bus, and whether or not their house value is worth over an average value of 400k.
  
  # PROJECT #3 REPORT
  
 On PDF file within this folder
