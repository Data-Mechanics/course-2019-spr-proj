import dml
import json
auth = json.load(open('../auth.json', 'r'))
import yelpfusion as yf
API_KEY= auth['services']['yelpfusionportal']['key']
import urllib.request

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')

r = list(repo['ctrinh_fat60221_veeyn.stations'].find({}))

print(r[0]["Green Line C"][0]["address"])

print(len(r[0]["Green Line B"]))

# r = {}

for i in range(len(r[0]["Green Line B"])):
    rd = {}
    res = yf.search(API_KEY, "coffee shop", r[0]["Green Line B"][i]["address"])
    for j in range(len(res['businesses'])):
        name = res['businesses'][num]['name'].replace(".", "")
        rd[name] = res['businesses'][num]['coordinates']
    print(res)
    print()

# print(res)

# for num in range(len(res['businesses'])):
#     name = res['businesses'][num]['name'].replace(".", "")
#     r[name] = res['businesses'][num]['coordinates']

# print(r)