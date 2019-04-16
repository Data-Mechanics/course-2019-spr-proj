###cs504 Project 2 Modeling, Optimization, and Statistical Analysis
Project Requirements & Important Date: (https://cs-people.bu.edu/lapets/504/s.php?#project2)
####Team Formation
- **JiaJia shen (jshen97@bu.edu)**
- **Shiwei Chen (leochans@bu.edu)**
- **Yuanpei Wang (wangyp@bu.edu)**
    - Chengyang He (henryhcy@bu.edu) has dropped the course

####Obtained Datasets and Sources
- Cvs stores within 15 km boston area through querying Google Places API (Search Nearby) 
    - (https://maps.googleapis.com/maps/api/place/nearbysearch/json?)
- Walgreen stores within 15 km boston area through querying Google Places API (Search Nearby)
    - (https://maps.googleapis.com/maps/api/place/nearbysearch/json?)
- Streetlight Locations in boston area through querying Boston Data Portal (Analyze Boston)
    - (https://data.boston.gov/api/3/action/datastore_search?resource_id=c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5)
- Eviction Incidents in boston area
    - (http://datamechanics.io/data/evictions_boston.csv)
- Crime Incidents in boston area
    - (http://datamechanics.io/dadta/crime.csv)
- MBTA stops in boston area
    - (http://datamechanics.io/data/MBTA_Stops.json)
    
####General Description & Narrative
The project is divided into two tasks:
- (Yuanpei Wang) Find a way to quantify the competitive relationship between the two of most popular franchising convenience stores in boston area: **CVS** and **Walgreen**

    1. Explore the location selection of CVS and Walgreen
        1. For each CVS store and Walgreen store, find its closest CVS and Walgreen and record the distance
        2. Using statistical analysis to explore the competitive relationship between Walgreen and CVS
    2. Optimization problem: If we want to locate n new CVS stores or Walgreen stores, how can we maximize its influence.
        1. CVS: we want the location of the new CVS stores to minimzie its distance between other Walgreen stores in order to increase its competitiveness against its opponent. Do the same for Walgreen stores

    

- (Shiwei Chen) Let's model the **Stability S** of a store **i** is defined by **S_i = 1/(|E||C|)** where **E** and **C** are the number of eviction cases and crimes cases that are assigned to store **i**:
    1. Explore the correlation of store ratings and Evictions/Crimes cases, which one has more effect on ratings?
        - Key Concepts: **Correlation Coefficient**, **permutation&p-values**
    2. Use the results obtained from the previous stage to refine the model of **Stability S**.
        - For example, if crime cases are more correlated with the ratings, then **S_i = 1/(|E|(|C|^2))**.
    3. Constraints Satisfaction/Optimization question: **If we have the chance to send n (n<60) salesmen (1 per store), how can we get the maximum total stability and maximum accessibility? 
        - Accessibility is defined as the sum of the distances between each pair of salesmen.
        - Key Concepts: **Constraints Satisfaction/Optimization**