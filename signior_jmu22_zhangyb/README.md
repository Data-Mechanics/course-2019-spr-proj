# project one
Members: 
  Kevin Xia (signior@bu.edu, kevinxia787)
  Jeffrey Mu (jmu22@bu.edu, jmu22)
  

The question we are trying to answer is how big of a factor (if any) does establishing a power plant at a specific year that burns either waste, oil, coal, gas has any effect on the current and subsequent years' statistics of CO2 emissions, Earth surface temperatures, Ocean surface temperatures, and Sea level changes. Depending on how many power plants that burn fossil fuels, we should be able to see drastic changes as more and more power plants get initialized. The reasoning behind this intuition is that since power plants that burn fossil fuels release waste into the air, if they have an effect on the CO2 emissions then we'll see gradual increases in CO2 as the years go by, and this value could be scaled based on how many power plants get comissioned. 

There is general consensus that fossil fuel based power plants negative impact the Earth's climate; however there are still members of the government and general community that think climate change is fake and/or power plants have no effect on it. By analyzing the data in this project (for a future time) we should be able to determine whether or not power plants have an effect on different factors used to measure Earth's climate.


# project two
Members:
  Kevin Xia (signior@bu.edu, kevinxia787)
  Jeffrey Mu (jmu22@bu.edu, jmu22)
  Yibin Zhang (zhangyb@bu.edu, JasonZhangyb)

For project two we are utilizing the same datasets we retrieved in project one. The question we want to answer is whether or not power plant establishment is a main driving force in the increase in metric tons of CO2 emitted by country. Before we began with constraint satisfaction, we had to retrieve our power plant data set, carbon emissions dataset, and create a new dataset for population data. The reason we needed population data was because the emissions data for CO2 was in metric tons per capita. We then wanted to do a constraint satisfaction - we attempted to ensure that all countries and subsquently all years that power plants were established in that country had their metric ton of CO2 increase per year less than the year after a power plant was established (e.g. if a power plant was established in Country X at year Yi, and let Zi be the metric tons of CO2 emitted at Yi, then we want to ensure that every country satisfies percentage_change(Zi-1, Zi) < percentage_cahange(Zi, Zi+1)). If this were true for all countries, we can assume that the establishment of a powerplant has an positive effect on the carbon emissions for that country. 

Our results for the constraint satisfaction didn't support this, as some countries had their CO2 emissions increase after a power plant was established, and some did not. In order to back this up with more data, we performed a linear regression on the data for each country, where the X value was the emissions before a power plant was established and the Y value was the emissions after a power plant was established, and calculated the correlation coeffecient with all of the before a powerplant was established values and all of the after a powerplant was established values. 

Our conclusions for this project basically reflect how the world is naturally. In the beginning we wanted to prove that there is a significant impact from fossil fuel based powerplants on carbon emissions. Based on the results of project two, we can conclude that power plants have an effect, but not to the extent we thought initially. The reasoning behind this is there are other things that could impact carbon emissions (e.g. transportation (cars, trains, planes, etc)). 



Instructions:

Some files depend on Datahub.io's datapackage package. Install it with:

  pip install datapackage


  