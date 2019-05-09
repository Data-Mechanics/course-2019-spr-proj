
# Chicago Transit Zones
Nathaniel Smith | BU: smithnj | github: njsmithh </br>
CS504 - Data Mechanics

## Intent
The City of Chicago currently has a flat rate of $2.50 for entry into the Elevated Rail ("L") transit system. Other metropolitan transit systems such as Transport for London's Tube network have fare zones where, depending on your entry and exit into the system, a fare may be higher or lower. While the "L" infrastructure is currently incapable of tracking when passengers exit the system, is it still possible for Chicago to benefit from zone-based fares?

Using data on station popularity, community socioeconomic hardship, and taxi pick-up and drop-off, fare zones can be created for the "L" network that encourage transit in burdened areas of the network while offsetting this fare deficit by charging riders in more stressed parts of the network a higher fare.

### Would a zone based system for the 'L' benefit Chicago?
*To answer this question, three goals are presented:*
1. **How are the zones determined?** Use k-means to create varying zones, taking into account station popularity, community hardship, and area taxi demand. Each k-means cluster represents a zone, with all stations residing in a zone.
2. **What would the zones look like?** Web-based visualizations are created to demonstrate a CTA map with the new zones. 
3. **Are the metrics used significant?** Web-based visualizations of the metrics are created to demonstrate their relation to each other. Correlation tests on metrics are also performed.

---
## Project 3 Features: <br>
See `"poster.pdf"` for the final project poster and `"CS504_NathanielSmith.pdf"` for the final project writeup. For high-quality versions of the interactive visualizations, see the `interactive_visualizations` folder.

---
## Scripts
| Name                     | Purpose                                                                    | Datasets/Scripts Used                                                  |
|--------------------------|----------------------------------------------------------------------------|----------------------------------------------------------------|
| create_taxiagg           | Calculate taxi ride totals for Community Areas.                            | get_taxitrips                                                  |
| create_stationpopularity | Calculate station popularity statistics.                                   | get_stationstats                                               |
| create_stationhardship   | Match Community Hardship Index with stations.                              | get_censushardship get_stations                                |
| create_metricarray       | Create final metric array of taxi rides, station popularity, and hardship. | create_taxiagg create_stationpopularity create_stationhardship |
| do_kmeans                | Perform k-means analysis on metric array to gather stations into clusters. | create_metricarray                                             |
| do_stats                 | Calculate metric coefficients.                                                               | create_metricarray                       |
| create_graphs | Generate 3D scatter plot of metrics after k-means algorithim analysis. | kmeans.data kmeans.centers |
| create_maps | Generate Folium maps of L-Station Fare Zones | zones
---
## Data Sets
| Portal             | Name (Source Linked)                                                                                                                 | Filetype |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------|----------|
| Chicago Data Porta | [Census Tract Boundaries](https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Census-Tracts-2010/5jrd-6zik)   | .geojson |
| Chicago Data Porta | ['L' Station Statistics](https://data.cityofchicago.org/Transportation/CTA-Ridership-L-Station-Entries-Monthly-Day-Type-A/t2rn-p8d7) | .csv     |
| Data.gov           | [Census Hardship Data](https://catalog.data.gov/dataset/census-data-selected-socioeconomic-indicators-in-chicago-2008-2012-36e55)    | .json    |
| Data.gov           | [Chicago Taxi Trip Data](https://catalog.data.gov/dataset/taxi-trips)                                                                | .json    |
| datamechanics.io   | [Chicago L-Stations](https://data.cityofchicago.org/Transportation/CTA-L-Rail-Stations-kml/4qtv-9w43)                                | .geojson |
| datamechanics.io   | [Chicago L-Lines](https://data.cityofchicago.org/widgets/53r7-y88m)                                | .geojson |
---
#### Library Dependencies
* pandas, geopandas, numpy
* JSON
* Data Mechanics Library, Provenance, Protoql
* datetime
* sodapy import Socrata
* matplotlib.pyplot
* sklearn
* mpl_toolkits.mplot3d
* from scipy.stats.stats import pearsonr
* plotly
* pymongo
