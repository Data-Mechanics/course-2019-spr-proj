import prov.model
import dml
import datetime
import pandas as pd
import json
import uuid
import urllib.request as ur
import sys
import zlib
from lxml import etree
from io import StringIO


def scrape_csv_url():
    systype = sys.getfilesystemencoding()
    base_url = "http://electionstats.state.ma.us"

    start_url = "http://electionstats.state.ma.us/ballot_questions/search/year_from:2002/year_to:2018/type:is_amendment%7Cis_initiative_petition%7Cis_referendum"
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9,zh;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'electionstats.state.ma.us',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
    }

    req = ur.Request(start_url, headers=headers)
    response = ur.urlopen(req)
    contents = response.read()
    response.close()

    def decompress_according_response(response, contents):
        res = contents
        if response.info().get('Content-Encoding'):
            res = zlib.decompress(contents, 16+zlib.MAX_WBITS)

        if response.info().get('Content-Type').find('charset') >= 0:
            index = response.info().get('Content-Type').find('charset')
            datatype = response.info().get('Content-Type')[index+8:].strip()
            res = res.decode(datatype, errors='replace')

        return res


    contents = decompress_according_response(response, contents)
    parser = etree.HTMLParser()
    html = etree.parse(StringIO(contents), parser)

    a_elements = html.xpath('//*[@class="more"]/@href')
    question_pages_url = [base_url + link for link in a_elements]
    question_pages_url = list(set(question_pages_url))

    all_csv_links = []
    for page_url in question_pages_url:
        req = ur.Request(page_url, headers=headers)
        response = ur.urlopen(req)
        contents = response.read()
        response.close()

        contents = decompress_according_response(response, contents)
        parser = etree.HTMLParser()
        html = etree.parse(StringIO(contents), parser)

        url_of_csv = html.xpath('//*[@class="download_csv"]/@href')
        all_csv_links.append(base_url + url_of_csv[1])


    return all_csv_links


class fetchCSVLink(dml.Algorithm):
    contributor = 'stathisk_simonwu_nathanmo_nikm'
    reads = []
    writes = ['stathisk_simonwu_nathanmo_nikm.links']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')

        tag = 'links'

        # 24 links of csv
        links = scrape_csv_url()
        df = pd.DataFrame(data={'link': links})
        json_mat = df.to_json(orient='records')
        repo.dropCollection(tag)
        repo.createCollection(tag)
        repo['stathisk_simonwu_nathanmo_nikm.' + tag].insert_many(json.loads(json_mat))
        repo['stathisk_simonwu_nathanmo_nikm.' + tag].metadata({'complete': True})

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        ''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('ballot', 'http://electionstats.state.ma.us/')

        this_script = doc.agent('alg:stathisk_simonwu_nathanmo_nikm#scrape_csv_url',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:web_pages',
                              {'prov:label': 'web page', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_links = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_links, this_script)

        doc.usage(get_links, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        links = doc.entity('dat:stathisk_simonwu_nathanmo_nikm#links',
                          {prov.model.PROV_LABEL: 'links of csv files', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(links, this_script)
        doc.wasGeneratedBy(links, get_links, endTime)
        doc.wasDerivedFrom(links, resource, get_links, get_links, get_links)

        repo.logout()
        return doc

if __name__ == '__main__':
    fetchCSVLink.execute(False)