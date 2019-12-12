import time
import pymysql
import logging
import os
import datetime
import sys
import datetime
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

logger_source1 = logging.getLogger('get_html_issue')
logger_source1.setLevel(logging.DEBUG)
fh_source1 = logging.handlers.TimedRotatingFileHandler(loggerPath +
                                                       "get_html_issue.log", when='midnight', interval=1,
                                                       backupCount=10)
fh_source1.setLevel(logging.DEBUG)
fh_source1.setFormatter(formatter)
logger_source1.addHandler(fh_source1)
logger_source2 = logging.getLogger('code_break_issue')
logger_source2.setLevel(logging.DEBUG)
fh_source2 = logging.handlers.TimedRotatingFileHandler(loggerPath +
                                                       "code_break_issue.log", when='midnight', interval=1,
                                                       backupCount=10)
fh_source2.setLevel(logging.DEBUG)
fh_source2.setFormatter(formatter)
logger_source2.addHandler(fh_source2)

print str(datetime.datetime.now())

def main():
    start = time.time()
    #options = Options()
    #options.add_argument('-headless')
    #driver = webdriver.Firefox(executable_path="/home/liu433/geckodriver",options=options)

    db = pymysql.connect(host='macquest2.cas.mcmaster.ca',
                         user='Macquest',
                         password='Macquest2@',
                         db='GasinfoProject')

    # Call for the Scraper API
    myScrape = scraper.scraper()
    #file1 = rename_log()
    # Read the data from the txt file
    file = open(base_dir + "Dataset_google.txt", "r")
    cid_list = []

    for i in file.readlines():
        cid = i.split("|")[-1].strip()
        cid_list.append(cid)
        # download the html raw data from the google map website
        html = myScrape.getWebPage(cid)
        if html['status'] != 0:
            print ("issue")
            logger_source1.info("Get hourly price: https://maps.google.com/?cid=%s detail: %s" % (cid,html['msg']))
        else:
            html = html['msg']
            price =  myScrape.getGasPrices(cid, html)
            print (price)
            if price['status'] != 0:
                logger_source2.info("Get hourly price: https://maps.google.com/?cid=%s detail: %s" % (cid,html['msg']))
            else:
                sql = price_update(price['msg'], cid, db)
    #driver.close()
    file.close()
    db.close()
    end = time.time()
    print(end - start)
    return 0

#def rename_log():
#    now = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M")
#    try:
#        new_name = "rt_info" + now + ".log"
#        os.rename( base_dir + "rtlog/rt_info.log", base_dir + "rtlog/"+ new_name)
#    except:
#        file = open(base_dir + "rtlog/rt_info.log", "w")
#        file.close()
#    file = open(base_dir + "rtlog/rt_info.log", "w")
#    file.close()
#    return file


def price_update(price, cid, db):
    cursor = db.cursor()
    try:
        if len(price['Premium']) != 1:
            pre = price['Premium'].replace('$', '')
        else:
            pre = 'NULL'

        if len(price['Regular']) != 1:
            reg = price['Regular'].replace('$', '')
        else:
            reg = 'NULL'

        if len(price['Diesel']) != 1:
            die = price['Diesel'].replace('$', '')
        else:
            die = 'NULL'

        if len(price['Midgrade']) != 1:
            mid = price['Midgrade'].replace('$', '')
        else:
            mid = 'NULL'
    except Exception as e:
        return 0

    try:
        sql = """INSERT INTO Priceinfo_google_decimal (stationid, premium, midgrade, regular, diesel) \
                 VALUES ('%s',%s,%s,%s,%s)""" \
                 % (cid, pre,mid,reg,die)
        cursor.execute(sql)
        print (sql)
        db.commit()
        return 0
    except Exception as e:
        cursor.rollback()
        print str(e)
        logger_source2.info('SQL Injection: %s : https://maps.google.com/?cid=%s'% (str(e),cid))
        return 0

if __name__ == '__main__':
    main()
