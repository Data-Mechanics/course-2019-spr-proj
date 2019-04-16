###cs504 Project 2 Modeling, Optimization, and Statistical Analysis

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
- **File rlated**: DataRetrieval.py
    
####General Description, Procedure, and Narrative
The project is divided into two tasks:
- **Task One: Quantify Competition**
    - Data
        1. Explore the location selection of CVS and Walgreen
        2. For each CVS store and Walgreen store, find its closest CVS and Walgreen and record the distance
        3. Using statistical analysis to explore the competitive relationship between Walgreen and CVS
    - Optimization problem: If we want to locate n new CVS stores or Walgreen stores, how can we maximize its influence.
        1. CVS: we want the location of the new CVS stores to minimzie its distance between other Walgreen stores in order to increase its competitiveness against its opponent. Do the same for Walgreen stores
* **Task Two: CVS, Larceny, and Eviction in the Central Boston**
    1. Let's assume and model that the **Stability S** of a store **i** is defined by **S_i = 1/(|E_i||L_i|)** where **|E_i|** and **|L_i|** are the number of eviction cases and the number of larceny cases that are related to store **i**:
    2. Among all type of crimes in the dataset crimes (Simple Assault, Battery, GTA, etc.), we assume that Larceny relates with the convenient store in the most direct way. By obtaining all eviction(**14k**) & larceny(**22k**) cases within **5.5km** of central boston, we successfully clustered all of them against the cvs stores(**18**) within **5km** of central boston.
        - **Note**: The extra **0.5km** is for those cvs stores located near the **5km** boundary.  
        - **Related Files**: CvsWalEviction.py; CvsWalCrime.py; CountEvictionCrimesCVS.py
        - **Key Library**: geopy.distance
        - **Key Concepts**: Data clustering and relational operations.
        - **Problems Encountered**: At the beginning, I was too ambitious. I wanted to include all cvs stores(**60**), eviction cases(**25k**), and crime cases(**270k**) within **15km** of central boston. That resulted in many useless data points where there are **0** crime/eviction cases. Worse than that, the size of data and the relatively slow computation of geo distance required a fairly long time(**7+ hrs**) to run. The run failed when the mongodb cursor was exhausted/timeout-ed. Large and comprehensive do not necessarily mean good in data science. The solution is to constrain our area of interest down to **5km** from the center of boston.
    3. In this step, the rating of a store are put together with the store's number of eviction/larceny cases. The data sets are refined before computation. The result stored in collection **correlationCVS** shows that rating and eviction cases have a pearson coefficient of **0.39326** with a **0.26090** p-value; the rating and Larceny cases have a pearson coefficient of **-0.20018** with a **0.44109** p-value. Neither of them have enough evidence to reject the null hypothesis (no correlation). However, by just looking at the c.c., it is also fair to explain that Eviction implies financially instability therefore affects rating disproportionately. Larceny instead improves the security level of the store therefore can slightly affect rating proportionally. Following this explaination, we refined the model of **S** such that: **S_i = ( (|L_i|^(c))/(|E_i|) ) times 1000**, where **c = |rho(rating, larceny)|/|rho(rating, eviction)|**.
        - **Note**: This step is not necessary the prerequisite of the Optimization/ConstraintSatisfaction problems. Just an exploration of the data.
        - **Related Files**: CorrelationCVS.py
        - **Key Library**: scipy.stats
        - **Key Concepts**: Statistical Analysis. Correlation and P-values.
        - **Problems Encountered**: Some outliers (i.e. with less than 100 cases) make the p-value too high to be useful. We excluded the outliers and converted the store rating to 100 score based. 
    4. Optimization: 
        - **Related Files**: Salesmen.py
        - **Question**: If we have the chance to send **3** salesmen (**1** per store), obtained the model that has the maximum (total stability **S**, total accessibility **A**) pair?
        - **Definition**: 
            - Accessibility is defined as the sum of the distances between each pair of salesmen, or each pair of stores that are assigned a salesman.
            - (S1, A1) > (S2, A2) if S1>S2 and A1>A2
        - **Results**: 
            - the 3 CVS are located at 
                - **210 Border St, East Boston**
                - **101 Canal St suite A, Boston**
                - **1249 Boylston St, Boston**
            - with a total stability **S =  0.4886**
            - with a total accessibility **A = 11.2147 km**
            - ![Google Maps result](Project2%20result.JPG)
    5. Constraints Satisfaction:
        - **Related Files**: Salesmen.py
        - **Question**: If we have the chance to send **3** salesmen (**1** per store), obtained the model that has the total stability >= **S**, total accessibility >= **A**?
        - **Definition**: 
            - Accessibility is defined as the sum of the distances between each pair of salesmen, or each pair of stores that are assigned a salesman.
        - **Results**:
            - With S := 0.6; A := 6.0, a possible set of 3 CVS are:
                - **101 Canal St suite A, Boston**
                - **1249 Boylston St, Boston**
                - **631 Washington St, Boston**
            - with a total **S = 0.63646**
            - with a total **A = 7.91282**
            - ![Google Maps result](Project2%20resultz3.JPG)
            - the result shows that if we want S as high as 0.6 and we don't care about A, then the result has covered the busiest commercial zone of boston.