import pymongo

def setup_mongodb():
    client = pymongo.MongoClient(host="localhost",port=27017)
    repo = client['repo']
    repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')
    return repo

def logout(repo):
    repo.logout()