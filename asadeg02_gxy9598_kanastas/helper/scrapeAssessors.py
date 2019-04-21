import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
import time
import selenium
from selenium import webdriver


class scrapeAssessors():       
  

    @staticmethod
    def scrapeAssessors(address_list):


        start_time = time.time()
        print("Scraping Boston gov for details of properties")
        #driver = webdriver.Chrome("/usr/bin/chromedriver")
        #driver.get("https://www.cityofboston.gov/assessing/search/")

        results = []    

        for address in address_list:

            driver = webdriver.Chrome("/usr/bin/chromedriver")
            driver.get("https://www.cityofboston.gov/assessing/search/")

            search_field = driver.find_element_by_xpath("//input[@type='search']")
            search_field.send_keys(address)

            submit = driver.find_element_by_xpath("//input[@type='submit']")
            submit.click()
            
            if len(driver.find_elements_by_tag_name("table")) >= 4:
                table = driver.find_elements_by_tag_name("table")[3]               

                rows = table.find_elements_by_tag_name("tr")

                for row in rows:

                    columns = (row.find_elements_by_tag_name("td"))
                    data = {}
                    data_keys = ["PARCEL ID", "ADDRESS", "OWNER", "VALUE"]
                    for i in range (0, len(columns) -2):
                        data[data_keys[i]] = columns[i].text        
                        if len(data.keys()) > 0:
                            results.append(data)
            driver.close()

        elapsed_time = time.time() - start_time
        print("Time elapsed: " + str(elapsed_time))
        print("Finished scraping Boston gov for details of properties")
        return results 
    