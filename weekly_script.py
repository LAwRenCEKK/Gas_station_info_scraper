import time
import pymysql
import sys
import datetime
import logging
from scraper import scraper

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.firefox.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

sys.dont_write_bytecode = True
base_dir = "/u50/liu433/gasinfo/"

loggerPath = "/home/liu433/gasinfo/loggings/"
formatter = logging.Formatter('%(asctime)s - %(message)s')

logger_source1 = logging.getLogger('monthly_scripting_issue')
logger_source1.setLevel(logging.DEBUG)
fh_source1 = logging.handlers.TimedRotatingFileHandler(loggerPath +
                                                       "monthly_scripting_issue.log", when='midnight', interval=1,
                                                       backupCount=10)
fh_source1.setLevel(logging.DEBUG)
fh_source1.setFormatter(formatter)
logger_source1.addHandler(fh_source1)

def main():
    start = time.time()
    # Database Connection
    print str(datetime.datetime.now())
    db = pymysql.connect(host='macquest2.cas.mcmaster.ca',
                         user='Macquest',
                         password='Macquest2@',
                         db='GasinfoProject')
    # Call for the Scraper API
    myScrape = scraper.scraper()
    file = open(base_dir + "Dataset_google.txt", "r")
    cid_list = []
    for i in file.readlines():
        cid = i.split("|")[-1].strip()
        cid_list.append(cid)
        # download the html raw data from the google map websit
        html = myScrape.getWebPage(cid)
        if html['status'] != 0:
            print "exception: %s"%(html['msg'])
            logger_source1.info("exception: %s" % (html['msg']))
        else:
            html = html['msg']
            # Get the popular time information
            station_info = myScrape.get_station_info(cid, html)
            if station_info['status'] == 0:
                print station_info_update(station_info['msg'], db)
            elif station_info['status'] == 1:
                print station_info['msg']
                logger_source1.info(station_info['msg'])
    file.close()

    options = Options()
    options.add_argument('-headless')
    driver = webdriver.Firefox(executable_path= "/home/liu433/geckodriver",options=options)
    file = open(base_dir+"map.txt","r")
    contents = []
    print ("Hi, before the for loop")
    for i in file.readlines():
        items = []
        stationid = i.split(",")[0].replace('"','')
        cid = i.split(",")[1].replace('"','')
        url = ("https://www.gasbuddy.com/Station/%s"%stationid)
        driver.get(url)
        features = driver.find_elements_by_css_selector("div.Amenity__amenityItem___19mEU")
        feature = ""
        print ("Hi, insider the  for loop")
        for i in features:
            feature = feature + " - " + i.text
        contents.append({"stationid":cid, "contents": feature[3:]})
        print (feature)
    print (contents)
    station_feature_update(contents,db)
    file.close()
    driver.close()
    db.close()

def station_feature_update(station_feature, db):
    cursor = db.cursor()
    for item in station_feature:
        try:
            sql = """INSERT INTO Station_features (stationid, contents)
                     VALUE ('%s', '%s')""" % (item['stationid'], item['contents'])
            cursor.execute(sql)
            db.commit()
        except Exception as e:
            db.rollback()
            print (str(e))
            logger_source1.info("SQL Issue: stationid: %s"%(sql))

            
def station_info_update(station_info, db):
    cursor = db.cursor()
    try:
        stationid = station_info['stationid']
        brand = station_info['brand']
        openhour = station_info['openhour']
        address = station_info['address']
    except Exception as e:
        return str(e)
    
    try:
        sql = """INSERT INTO Station_info (stationid, openhour, address, brand) \
                 VALUES ('%s','%s','%s','%s')""" \
                 % (stationid, openhour, address, brand)
        cursor.execute(sql)
        db.commit()
        return 0
    except Exception as e:
        # Rollback in case there is any error
        db.rollback()
        logger_source1.info("https://maps.google.com/?cid=%s detail: %s" % (cid, str(e)))
        return str(e)

if __name__ == '__main__':
    main()
