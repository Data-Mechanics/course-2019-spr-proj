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
collection = repo["stathisk_simonwu_nathanmo_nikm.republican_primary"]

#query avg answers for top 10
data = collection.find()
trump = []
rubio = []
cruz = []
town = []
sumsAndTown = {}

for i in range(data.count()):
    entry = data[i]
    if entry["City/Town"] == 'TOTALS':
        continue

    trump.append(int(entry['Donald J Trump'].replace(',', '')))
    rubio.append(int(entry['Marco Rubio'].replace(',', '')))
    cruz.append(int(entry['Ted Cruz'].replace(',', '')))
    town.append(entry["City/Town"])
    sumsAndTown[entry["City/Town"]] = int(entry['Donald J Trump'].replace(',', '')) \
                                     + int(entry['Marco Rubio'].replace(',', '')) + \
                                      int(entry['Ted Cruz'].replace(',', ''))

topTowns = []
while len(topTowns) != 10:
    maxTown = max(sumsAndTown.items(), key=operator.itemgetter(1))[0]
    topTowns.append(maxTown)
    sumsAndTown.pop(maxTown, None)

topTrump = []
topRubio = []
topCruz = []

for topTown in topTowns:
    topTrump.append(trump[town.index(topTown)])
    topCruz.append(cruz[town.index(topTown)])
    topRubio.append(rubio[town.index(topTown)])





ind = np.arange(len(topTowns))  # the x locations for the groups
width = 0.35  # the width of the bars
fig, ax = plt.subplots()

rects1 = ax.bar(ind - width/2, topTrump, width,
                color='SkyBlue', label='Donald Trump')
rects2 = ax.bar(ind + width/2, topRubio, width,
                color='IndianRed', label='Marco Rubio')
rects3 = ax.bar(ind + width/2, topCruz, width,
                color='Green', label='Ted Cruz')

ax.set_ylabel('Votes')
ax.set_title('Repubilcan Primary Voter Distribution in Towns with Highest Voter Participation')
ax.set_xticks(ind)
ax.set_xticklabels(topTowns)
ax.legend()
plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')


#autolabel(rects1, "left")
#autolabel(rects2, "right")
plt.savefig('republicanPrimary.png')
plt.show()

