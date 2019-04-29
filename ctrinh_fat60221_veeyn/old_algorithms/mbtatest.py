import urllib.request
import json

url = 'http://cs-people.bu.edu/lapets/591/examples/lost.json'
response = urllib.request.urlopen(url).read().decode("utf-8")

r = json.loads(response)

# print(r[0]['latitude'])

s = json.dumps(r, sort_keys=True, indent=2)

# print(s)

stops = "https://api-v3.mbta.com/stops?filter[route]=Green-B"
sid = "place-lake"
schedules = "https://api-v3.mbta.com/schedules?filter[date]=2019-04-08&sort=-departure-time&filter[stop]=" + sid
# print(schedules)

resdict = {}
# print(resdict)

rindict = {}
# print(rindict)

response = urllib.request.urlopen(stops).read().decode("utf-8")

r = json.loads(response)

length = len(r['data'])

print('length of r', length)

import time

# print(r['data'][0]['id'])


### BAD TIME

# for i in range(length):
#     response = urllib.request.urlopen(stops).read().decode("utf-8")
#     r = json.loads(response)
#     rindict = {}
#     name = r['data'][i]['attributes']['name']
#     latitude = r['data'][i]['attributes']['latitude']
#     longitude = r['data'][i]['attributes']['longitude']
#     print(name)
#     sid = r['data'][i]['id']
#     # print(schedules)
#     # print(sid)
#     rindict['latitude'] = latitude
#     rindict['longitude'] = longitude
#     # print(rindict)
#     response = urllib.request.urlopen(schedules).read().decode("utf-8")
#     r = json.loads(response)
#     # print(r['data'][i]['attributes']['departure_time'])
#     res = r['data'][i]['attributes']['departure_time']
#     # print(res)
#     rindict['departure_time'] = res
#     resdict[name] = rindict
#     print(resdict)
#     time.sleep(2)

response = urllib.request.urlopen(stops).read().decode("utf-8")
r = json.loads(response)
reslist = []

for i in range(length):
    rindict = {}
    name = r['data'][i]['attributes']['name']
    sid = r['data'][i]['id']
    latitude = r['data'][i]['attributes']['latitude']
    longitude = r['data'][i]['attributes']['longitude']
    print(name)
    rindict['name'] = name
    rindict['stop_id'] = sid
    rindict['latitude'] = latitude
    rindict['longitude'] = longitude
    # orignal
    # resdict[name] = rindict
    reslist.append(rindict)
    # print(resdict)

# print(reslist[0])

resdict['data'] = reslist

print(len(resdict['data']))

print(resdict['data'][0])

print()

timelist = ["06","07","08","09","10","11"]

for i in range(len(resdict['data'])):
    # print(resdict['data'][i]['stop_id'])
    sid = resdict['data'][i]['stop_id']
    response = urllib.request.urlopen(schedules).read().decode("utf-8")
    r = json.loads(response)
    print("len", len(r['data']))
    lst = []
    for j in range(len(r['data'])):
        resrr = r['data'][j]['attributes']['departure_time']
        resch = resrr[11:13]
        if resch in timelist:
            lst.append(resrr)
        # print(lst)
    resdict['data'][i]['departure_times'] = lst
    resdict['data'][i]['peak_commute_departures'] = len(lst)
    time.sleep(5)

# print(resdict[0])

# response = urllib.request.urlopen(schedules).read().decode("utf-8")
# r = json.loads(response)

# for i in range(len(resdict)):
#     sid = resdict[i]['sid']
#     rindict = {}
#     name = r['data'][i]['attributes']['name']
#     sid = r['data'][i]['id']
#     latitude = r['data'][i]['attributes']['latitude']
#     longitude = r['data'][i]['attributes']['longitude']
#     print(name)
#     rindict['stop_id'] = sid
#     rindict['latitude'] = latitude
#     rindict['longitude'] = longitude
#     resdict[name] = rindict
#     print(resdict)

#     res = r['data'][i]['attributes']['departure_time']
#     rindict['departure_time'] = res


print()
print("FINAL:")
print()
outfile = open('deptimes.txt', 'w')
print(resdict, file=outfile)


# from datetime import datetime, timezone

# def utc_to_local(utc_dt):
#     return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

# import pytz

# tz = timezone('America/St_Johns')

# result = pytz.utc.localize(res, is_dst=None).astimezone(tz)

# print(result)

# print(utc_to_local(res))