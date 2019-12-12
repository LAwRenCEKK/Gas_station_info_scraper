import sys
import time
import pymysql
import logging
from logging.handlers import TimedRotatingFileHandler

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.firefox.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


logger1 = logging.getLogger('review_scraper')
logger1.setLevel(logging.DEBUG)
logger2 = logging.getLogger('review_scraper1')
logger2.setLevel(logging.DEBUG)
fh1 = logging.FileHandler('/home/liu433/gasinfo/loggings/review_issue.log')
fh2 = TimedRotatingFileHandler('/home/liu433/gasinfo/loggings/review_info.log', 
                                                 when='midnight', 
                                                 interval=1,
                                                 backupCount=10)
fh1.setLevel(logging.DEBUG)
fh2.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s-%(message)s')
fh1.setFormatter(formatter)
fh2.setFormatter(formatter)
logger1.addHandler(fh1)
logger2.addHandler(fh2)


def main():
    total_reviews = 0
    counter_duplicate = 0 
    counter_new = 0
    options = Options()
    options.add_argument('-headless')

    base_dir = "/u50/liu433/gasinfo/"
    driver = webdriver.Firefox(executable_path="/home/liu433/geckodriver",options=options)

    file = open(base_dir+"Dataset_google.txt","r")
    for i in file.readlines():
        try:
            cid = i.split("|")[-1].strip()
            driver.get("http://maps.google.com/?cid=%s"%cid)

            button_review = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.widget-pane-link")))

            num_reviews = button_review.text.split(" ")
            num_reviews = int(num_reviews[0])
            total_reviews = total_reviews + num_reviews
            button_review.click()
            
            element = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.section-listbox.section-scrollbox.scrollable-y.scrollable-show")))

            reviews_current = 0
            reviews_previous = 0
            counter_timeout = 0

            print ("\n%s:"%cid)

            # Scroll the page down to the bottom
            while (reviews_current < num_reviews):
                reviews_previous = reviews_current
                review = driver.find_elements_by_css_selector("div.section-review.ripple-container")
                reviews_current = len(review)
                print ("Curently process: %i of %i reviews"%(reviews_current,num_reviews))
                # To prevent the infinite loop caused by the network congestion
                if (reviews_current == reviews_previous):
                    counter_timeout = counter_timeout + 1
                    if counter_timeout > 20:
                        print ("Warning: infinite loop")
                        logger1.info("Station %s has infinite loop"%(cid))
                        break

                # Send key to scroll down
                element.send_keys(Keys.PAGE_DOWN)
                element.send_keys(Keys.PAGE_DOWN)
                element.send_keys(Keys.PAGE_DOWN)
                time.sleep(1.5)
                element.send_keys(Keys.PAGE_DOWN)

            # Call the other function
            get_reviewer(driver,cid,counter_duplicate,counter_new)

        except Exception as e:
            logger1.info("Station %s: %s"%(cid, str(e)))
            print ("Station %s: %s"%(cid, str(e)))    

    logger2.info("Current total number of reviews is %i"%total_reviews)
    logger2.info("Duplicated number of reviews is %i"%counter_duplicate)
    logger2.info("New reviews are  %i"%counter_new)

    print (total_reviews)
    driver.close()
    file.close()


def get_reviewer(driver,cid,counter_duplicate,counter_new):
    db = pymysql.connect(host='macquest2.cas.mcmaster.ca',
        user='Macquest',
        password='Macquest2@',
        db='GasinfoProject')

    reviewid = driver.find_elements_by_css_selector("div.section-review.ripple-container")         
    content = driver.find_elements_by_css_selector("span.section-review-text")
    reviewer = driver.find_elements_by_css_selector("div.section-review-title")
    date = driver.find_elements_by_css_selector("span.section-review-publish-date")
    rating = driver.find_elements_by_css_selector("span.section-review-stars")
    j = 0
    k = 0

    for i in range(len(content)):
        pkg = {
            "stationid": cid,
            "reviewid": reviewid[j].get_attribute("data-review-id"),
            "contents": content[j].text.replace("'",""),
            "reviewers": reviewer[k].text.replace("'",""),
            "publish_date": date[j].text,
            "score": rating[j].get_attribute("aria-label")
            }
        j = j + 1
        k = k + 2
        update_mysql(db,pkg,counter_duplicate,counter_new)

    db.close()
    

def update_mysql(db,pkg,counter_duplicate,counter_new):
    """Ingest data to the database table"""
    counter_duplicate = 0
    try:
        cursor = db.cursor()
        sql1 = """INSERT INTO Complete_reviews_new (reviewid, stationid, reviewers, contents,\
                  score, publish_date) \
                  VALUES ('%s','%s','%s','%s','%s','%s')"""\
                  %(pkg['reviewid'],pkg['stationid'],pkg['reviewers'],pkg['contents'],pkg['score'],pkg['publish_date'])
       
        sql2 = """INSERT INTO Complete_reviews_new_daily(reviewid, stationid, reviewers, contents,\
                  score, publish_date) \
                  VALUES ('%s','%s','%s','%s','%s','%s')"""\
                  %(pkg['reviewid'],pkg['stationid'],pkg['reviewers'],pkg['contents'],pkg['score'],pkg['publish_date'])

        cursor.execute(sql1)
        cursor.execute(sql2)

        db.commit()
        counter_new = counter_new + 1

    except Exception as e:
        counter_duplicate = counter_duplicate + 1
        db.rollback()
        logger1.info("SQL issues %s and %s"%(sql1, str(e)))
        print ("SQL issues %s"%str(e))


if __name__ == '__main__':
    main()

