# -*- coding: utf-8 -*-
import time
import pymysql
import sys
import datetime
import logging
from scraper import scraper
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
    whole_sale_price = myScrape.get_extra_info()
    if whole_sale_price['status'] != 0:
        print whole_sale_price
    load_rack_price(whole_sale_price['msg'], db) 
    # Read the data from the txt file
    file = open(base_dir + "Dataset_google.txt", "r")
    cid_list = []
    for i in file.readlines():
        cid = i.split("|")[-1].strip()
        cid_list.append(cid)
        # download the html raw data from the google map websit
        html = myScrape.getWebPage(cid)
        if html['status'] != 0:
            print html
            logger_source1.info("https://maps.google.com/?cid=%s detail: %s" % (cid,html['msg']))
        else:
            html = html['msg']
            # Get the popular time information
            popular_time = myScrape.busyhourCollectDaily(cid, html)
            if popular_time['status'] == 0:
                popular_time_update(cid, popular_time['msg'], db)
            elif popular_time['status'] == 1:
                print popular_time 
                logger_source2.info("Populartime_daily: https://maps.google.com/?cid=%s detail: %s" % (cid,popular_time['msg']))
            else:
                print popular_time
            # Get the number of photos, average rating, number of reviews
            info = myScrape.get_num_photo_review(cid, html)
            if info['status']==0:
                sql = info_update(cid, info['msg'], db)
            else:
                print info
                logger_source2.info("https://maps.google.com/?cid=%s detail: %s" % (cid, info['msg']))
            # Get top reviews
            top_reviews = myScrape.get_top_reviews(cid, html)
            if top_reviews['status'] == 0:
                if len(top_reviews['msg']):
                    review_update(cid, top_reviews['msg'], db)
            else:
                print top_reviews
                logger_source2.info("https://maps.google.com/?cid=%s detail: %s" % (cid, top_reviews['msg']))
    file.close()
    db.close()
    end = time.time()
    print(end - start)
    return 0


def popular_time_update(cid, popular_time, db):
    cursor = db.cursor()
    try:
        for i in popular_time:
            for j in i:
	        if (len(str(j[1]))<5):
	            sql = """INSERT INTO Popular_time_info(stationid, hour, frequency, weekday)\
                             VALUE ('%s','%s','%s','%s')"""\
                             % (cid, j[0], j[1], j[2])
                    cursor.execute(sql)
                    db.commit()
        return 0
    except Exception as e:
        db.rollback()
        print str(e)
        logger_source2.info("https://maps.google.com/?cid=%s detail: %s" % (cid, str(e)))


def review_update(cid, top_reviews, db):
    cursor = db.cursor()
    try:
        for i in top_reviews:
            sql = """INSERT INTO Recommended_reviews (stationid, contents)\
                     VALUES ('%s','%s')""" \
                     % (cid, i)
            cursor.execute(sql)
            db.commit()

    except Exception as e:
    # Rollback in case there is any erro
        db.rollback()
        print str(e)
        logger_source2.info("https://maps.google.com/?cid=%s detail: %s" % (cid, str(e))) 


def info_update(cid, info, db):
    cursor = db.cursor()
    try:
        photo = info['photo']
        review = info['review']
        rating = info['rating']

    except Exception as e:
        return str(e)
    try:
        sql = """INSERT INTO Aveage_score_num_photos (stationid, aveage_score, number_photo, total_review) \
                 VALUES ('%s','%s', '%s', '%s')""" \
                 % (cid, rating,photo, review)
        cursor.execute(sql)
        db.commit()
        return 0
    except Exception as e:
    # Rollback in case there is any erro
        db.rollback()
        print str(e)
        logger_source2.info("https://maps.google.com/?cid=%s detail: %s" % (cid, str(e)))


def load_rack_price(items, db):
    cursor = db.cursor()
    for i in range(len(items)):
        sql = """INSERT INTO Wholesale_price (location, REG_87, MID_89, SUP_91,\
                 REG_E‑10, MID_E‑5, ULS_Diesel, ULS_Diesel1,Furnace_Oil, Stove_Oil) \
                 VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""\
                 % (items[i][0], items[i][1], items[i][2], items[i][3], items[i][4], \
                 items[i][5], items[i][6], items[i][7], items[i][8], items[i][9])
        try:
        # Execute the SQL command
            cursor.execute(sql)
        # Commit your changes in the database
            db.commit()
        except Exception as e:
        # Rollback in case there is any error
            db.rollback()
            logger_source2.info("https://maps.google.com/?cid=%s detail: %s" % (cid, str(e)))


if __name__ == '__main__':
    main()
