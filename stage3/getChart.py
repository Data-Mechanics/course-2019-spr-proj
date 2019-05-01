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
#myquery = { "address": { "$gt": "S" } }
data = collection.find({"Locality" : "Boston"})
yes = int(data[0]['Yes'])
no = int(data[0]['No'])
blanks = int(data[0]['Blanks'])

objects = ('Yes', 'No', 'Blank')
y_pos = np.arange(len(objects))
performance = [yes, no, blanks]

plt.bar(y_pos, performance, align='center', alpha=0.5)
plt.xticks(y_pos, objects)
plt.ylabel('Votes')
plt.title('Average Answer of Boston Voters')

plt.savefig('avgBoston.png')
plt.show()


