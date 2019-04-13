# This function take two variables
# First variable: home address.  Second variable: work address
# Both are strings.  For example, '418 Beachview Drive, North Vancouver, BC'
# This func uses googlemaps distance_matrix API to calculate travel time
# The return is a string.  for example, 26 mins.

import googlemaps
from datetime import datetime
# import json
# from pprint import pprint

# home = '418 Beachview Drive, North Vancouver, BC'
# work = '138 East 7th Avenue, Vancouver, BC'
#gmaps = googlemaps.Client(key='AIzaSyBknqIxQcdcDMT-po3pNYat7dj0yH7fLeE')
#time = datetime.now()

# PUT IN API KEY HERE
api_key = ''


def time(home_addr, work_addr):
    gmaps = googlemaps.Client(key=api_key)
    time = datetime.now()
    commute_json = gmaps.distance_matrix(origins=home_addr, destinations=work_addr)
    commute_time = commute_json['rows'][0]['elements'][0]['duration']['text']
    return commute_time


def distance(home_addr, work_addr):
    gmaps = googlemaps.Client(key=api_key)
    time = datetime.now()
    commute_json = gmaps.distance_matrix(origins=home_addr, destinations=work_addr)
    commute_distance = commute_json['rows'][0]['elements'][0]['distance']['text']
    return commute_distance

if __name__ == '__main__':
#    home_addr = '418 Beachview dr. North Vancouver'
    #home_addr = '418 Beachview Dr, North Vancouver'
    home_addr = '509 Park Dr, Boston'
    #work_addr = '138 East 7th, Vancouver'
    work_addr = '800 Boylston St, Boston'
    ct_time = time(home_addr,work_addr)
    print(ct_time)
    ct_dist = distance(home_addr,work_addr)
    print(ct_dist)

# print(commute_json[0]["duration"])
# commute = json.load(commute_json)
# geocode_home = gmaps.geocode('418 Beachview Drive, North Vancouver, BC')
# geocode_work = gmaps.geocode('138 East 7th Avenue, Vancouver, BC')
# print(geocode_home)
# print(geocode_work)


# directions_result = gmaps.directions("Sydney Town Hall","Parramatta, NSW",mode="transit",departure_time=now)


# print(geocode_result)
# print(directions_result)