from pymongo import MongoClient
from web.tools import NEIGH_ZIP
client = MongoClient()
db = client.get_database("repo")


def cat_by_zip(zip_code):
    locs = db["kzhang21_ryuc_zui_sarms.yelp_business"].find(
        {"location.zip_code": str(zip_code)})
    r = []
    for rest in locs:
        rest.pop("_id")
        r.append(rest)
    return r


def cat_by_neigh(neigh):
    zips = list(map(str, NEIGH_ZIP.get(neigh, [])))
    if not zips:
        return []

    locs = db["kzhang21_ryuc_zui_sarms.yelp_business"].find(
        {"location.zip_code": {"$in": zips}})

    r = []
    for rest in locs:
        rest.pop("_id")
        r.append(rest)
    return r
