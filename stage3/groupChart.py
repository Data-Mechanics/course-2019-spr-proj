import pymongo
import dml
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt

dml.pymongo.MongoClient()
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')
collection = repo["stathisk_simonwu_nathanmo_nikm.avgAnswers"]

#query avg answers for top 10
data = collection.find().sort([("Total Votes Cast", pymongo.DESCENDING)])
yesList = []
noList = []
localityList = []
for i in range(10):
    entry = data[i]
    yesList.append(entry['Yes'])
    noList.append(entry['No'])
    localityList.append(entry["Locality"])

ind = np.arange(len(yesList))  # the x locations for the groups
width = 0.35  # the width of the bars
fig, ax = plt.subplots()

rects1 = ax.bar(ind - width/2, yesList, width,
                color='SkyBlue', label='Yes')
rects2 = ax.bar(ind + width/2, noList, width,
                color='IndianRed', label='No')

ax.set_ylabel('Votes')
ax.set_title('Average Vote Distribution on MA Towns with Top Ten Vote Counts')
ax.set_xticks(ind)
ax.set_xticklabels(localityList)
ax.legend()
plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')


#autolabel(rects1, "left")
#autolabel(rects2, "right")
plt.savefig('topTen.png')
plt.show()
