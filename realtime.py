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



print str(datetime.datetime.now())
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
    myscrape = scraper.scraper()

    db = pymysql.connect(host='macquest2.cas.mcmaster.ca',
                         user='Macquest',
                         password='Macquest2@',
                         db='GasinfoProject')
    # Read the data from the txt file
    myfile = open(base_dir + "Dataset_google.txt", "r")
    for cid in myfile.readlines():
        cid = cid.split("|")[3].strip()
        html = myscrape.getWebPage(cid)

        if html['status'] != 0:
            logger_source1.info("HTML issue-Popular time live: https://maps.google.com/?cid=%s"%cid)
        else:
            
            html = html['msg']
            rt_popular_time = myscrape.realtimeBusyhourCollect(cid, html)
            if rt_popular_time['status'] != 0:
                print rt_popular_time
            else:
                real_time_update(cid, rt_popular_time['msg'], db)
    myfile.close()
    db.close()
    end = time.time()
    print(end - start)
    print str(datetime.datetime.now())
    return 0


def real_time_update(cid, rt_popular_time, db):
    cursor = db.cursor()
    try:
        weekday = rt_popular_time['week']
        hour = rt_popular_time['hour']
        usual = rt_popular_time['usual']
        current = rt_popular_time['current']
    except Exception as e:
        logger_source2.info("SQL Ingestion issue: https://maps.google.com/?cid=%s  detail: %s"%(cid,str(e)))
        return 0
    try:
        sql = """INSERT INTO Popular_time_info_live (stationid, weekday, hour, currently, usually) \
                 VALUES ('%s','%s', '%s', '%s', '%s')""" \
                 % (cid, weekday, hour, current, usual)
        cursor.execute(sql)
        db.commit()
        return 0
    except Exception as e:
        db.rollback()
        print str(e)
        logger_source2.info("SQL Ingestion issue: https://maps.google.com/?cid=%s  detail: %s"%(cid,str(e)))
        return 0


if __name__ == '__main__':
    main()
