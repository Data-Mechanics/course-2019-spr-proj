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
- **File rlated**: DataRetrieval.py
    
####General Description, Procedure, and Narrative
The project is divided into two tasks:
- (Yuanpei Wang) Find a way to quantify the competitive relationship between the two of most popular franchising convenience stores in boston area: **CVS** and **Walgreen**
* **CVS, Larceny, and Eviction in the Central Boston --Shiwei Chen**
    1. Let's assume and model that the **Stability S** of a store **i** is defined by **S_i = 1/(|E||L|)** where **E** and **L** are the number of eviction cases and larceny cases that are assigned to store **i**:
    2. By the common senses, among all type of crimes, Larceny relates with the convenient store in the most direct way. By obtaining all eviction(**14k**) & larceny(**22k**) cases within **5.5km** of central boston, I clustered all of them against the cvs stores(**18**) within **5km** of central boston.
        - **Note**: The extra **0.5km** is for those cvs stores located near the **5km** boundary.  
        - **Related Files**: CvsWalEviction.py; CvsWalCrime.py; CountEvictionCrimesCVS.py
        - **Key Library**: geopy.distance.distance(coord1, coord2) to compute distance.
        - **Key Concepts**: Data clustering and relational operations.
        - **Problems Encountered**: At the beginning, I was ambitious. I wanted to include all cvs stores(**60**), eviction cases(**25k**), and crime cases(**270k**) within **15km** of central boston. That resulted in many of useless data point where there is **0** crime/eviction cases. Worse than that, the size of data and the relatively slow computation of geo distance required a fairly long time(**7+ hrs**) to run. The run ended when the mongodb cursor was exhausted/timeout-ed. So now I focused on a more small and concentrated area: **5km** from the center of boston.
    3. In this step, the data of ratings and E&C are refined and put together for computation. The result shows that rating and eviction cases have a pearson coefficient of **0.39326** with a **0.26090** p-value; the rating and Larceny cases have a pearson coefficient of **-0.20018** with a **0.44109** p-value. Neither of them have enough evidence to reject the null hypothesis (no correlation). However, I think we are able to conceive an explaination that Eviction implies financially instability can affect rating disproportionately. Larceny instead improves the security level of the store can slightly affect rating proportionally. Now the model of **S** can be changed to: **S_i = ( (|L_i|^(c))/(|E_i|) ) times 1000**, where **c = |rho(rating, larceny)|/|rho(rating, eviction)|**.
        - **Note**: This step is not necessary the prerequisite of the following steps.
        - **Related Files**: CorrelationCVS.py
        - **Key Library**: scipy.stats.pearsonr(x_list, y_list) to compute rho and p-value.
        - **Key Concepts**: Statistical Analysis. Correlation and P-values.
        - **Problems Encountered**: Some outlier (i.e. with less than 100 cases) makes the p-value too high to be useful. Now the outliers are excluded and ratings are converted to 100 score based. 
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
                - **"631 Washington St, Boston"**
            - total **S = 0.63646**
            - total **A = 7.91282**
            - ![Google Maps result](Project2%20resultz3.JPG)
            - Note that: with total S >= 0.6, the results has covered the busiest and most commerce-heavy zone of boston! Interesting.