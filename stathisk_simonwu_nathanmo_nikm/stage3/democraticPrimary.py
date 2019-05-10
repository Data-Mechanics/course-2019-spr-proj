import pymongo
import dml
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
import operator

dml.pymongo.MongoClient()
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')
collection = repo["stathisk_simonwu_nathanmo_nikm.democratic_primary"]

#query avg answers for top 10
data = collection.find()
hillary = []
bernie = []
town = []
sumsAndTown = {}

for i in range(data.count()):
    entry = data[i]
    if entry["City/Town"] == 'TOTALS':
        continue

    hillary.append(int(entry['Hillary Clinton'].replace(',', '')))
    bernie.append(int(entry['Bernie Sanders'].replace(',', '')))
    town.append(entry["City/Town"])
    sumsAndTown[entry["City/Town"]] =int(entry['Bernie Sanders'].replace(',', '')) + int(entry['Hillary Clinton'].replace(',', ''))

topTowns = []
while len(topTowns) != 10:
    maxTown = max(sumsAndTown.items(), key=operator.itemgetter(1))[0]
    topTowns.append(maxTown)
    sumsAndTown.pop(maxTown, None)

topBernie = []
topHilary = []

for topTown in topTowns:
    topBernie.append(bernie[town.index(topTown)])
    topHilary.append(hillary[town.index(topTown)])





ind = np.arange(len(topTowns))  # the x locations for the groups
width = 0.35  # the width of the bars
fig, ax = plt.subplots()

rects1 = ax.bar(ind - width/2, topHilary, width,
                color='SkyBlue', label='Hillary Clinton')
rects2 = ax.bar(ind + width/2, topBernie, width,
                color='IndianRed', label='Bernie Sanders')

ax.set_ylabel('Votes')
ax.set_title('Democratic Primary Voter Distribution in Towns with Highest Voter Participation')
ax.set_xticks(ind)
ax.set_xticklabels(topTowns)
ax.legend()
plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')


#autolabel(rects1, "left")
#autolabel(rects2, "right")
plt.savefig('demPrimary.png')
plt.show()

