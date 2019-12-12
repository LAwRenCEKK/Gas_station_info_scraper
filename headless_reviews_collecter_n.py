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
fh2 = logging.FileHandler('/home/liu433/gasinfo/loggings/review_info.log')
fh1.setLevel(logging.DEBUG)
fh2.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s-%(message)s')
fh1.setFormatter(formatter)
fh2.setFormatter(formatter)
logger1.addHandler(fh1)
logger2.addHandler(fh2)


def main():
    logger2.info("\nStarting:")
    total_reviews = 0
    options = Options()
    options.add_argument('-headless')
    base_dir = "/u50/liu433/gasinfo/"
    driver = webdriver.Firefox(executable_path="/home/liu433/geckodriver",options=options)
    file = open(base_dir+"Dataset_google.txt","r")
    # Get all existing reviewid 
    db = helper_database() 
    reviewid_set = db.get_reviewid_set()
    for i in file.readlines():
        try:
            cid = i.split("|")[-1].strip()
            #cid = '5404823653698154368'
            driver.get("http://maps.google.com/?cid=%s"%cid)
            # Press the Review button to direct to the review page
            button_review = WebDriverWait(driver, 15).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR,
                            "button.widget-pane-link")))
            num_reviews = button_review.text
            try:
                num_reviews = int(num_reviews.strip().replace("(","").replace(")",""))
            except ValueError:
                print "Can't find the button %s"%(button_review.text)
                logger1.info("Station %s: No review info"%(cid))
                continue
                #break
            button_review.click()
            # Scroll the page down to the bottom
            element = WebDriverWait(driver, 15).until(
                      EC.presence_of_element_located((By.CSS_SELECTOR,
                      "div.section-layout.section-scrollbox.scrollable-y.scrollable-show")))
            #          "div.section-listbox.section-scrollbox.scrollable-y.scrollable-show")))
            reviews_current = 0
            reviews_previous= 0
            counter_timeout = 0
            print ("%s:"%cid)
            while (reviews_current < num_reviews):
                reviews_previous = reviews_current
                review = driver.find_elements_by_css_selector("div.section-review.ripple-container")
                reviews_current = len(review)
                print ("Curent process: %i of %i reviews"%(reviews_current,num_reviews))
                if (reviews_current == reviews_previous):
                    counter_timeout = counter_timeout + 1
                    if counter_timeout > 20:
                        print ("Warning: infinite loop")
                        logger1.info("Station %s: infinite loop-process: %i of %i reviews"%\
                            (cid,reviews_current,num_reviews))
                        break
                element.send_keys(Keys.PAGE_DOWN)
                element.send_keys(Keys.PAGE_DOWN)
                element.send_keys(Keys.PAGE_DOWN)
                time.sleep(1)

            total_reviews = total_reviews + num_reviews
            # Page reaches to the bottom and start to collect the review info
            get_reviewer(driver,cid, reviewid_set, db)

        except Exception as e:
            logger1.info("Station %s: undefined exception-%s"%(cid, str(e)))
            print ("Station %s: undefined exception-%s"%(cid, str(e)))    
        except KeyboardInterrupt:
            driver.close()
    driver.close()
    logger2.info("Current total number: %i"%total_reviews)
    print (total_reviews)

def get_reviewer(driver,cid, reviewid_set, db):
    reviewid = driver.find_elements_by_css_selector("div.section-review.ripple-container")
    for i in range(len(reviewid)):
        review_id = reviewid[i].get_attribute("data-review-id")
        print (review_id)
        
        if review_id in reviewid_set:
            pass
        else:
            content = driver.find_elements_by_css_selector("span.section-review-text")
            reviewer = driver.find_elements_by_css_selector("div.section-review-title")
            date = driver.find_elements_by_css_selector("span.section-review-publish-date")
            rating = driver.find_elements_by_css_selector("span.section-review-stars")
            pkg = {
                "stationid": cid,
                "reviewid": review_id,
                "contents": content[i].text.replace("'",""),
                "reviewers": reviewer[2*i].text.replace("'",""),
                "publish_date": date[i].text,
                "score": rating[i].get_attribute("aria-label")
                }
            print (pkg)
            db.update_mysql(pkg)


class helper_database:
    config = {
            'host': 'macquest2.cas.mcmaster.ca',
            'port': 3306,
            'user': 'Macquest',
            'database':'GasinfoProject',
            'charset':'utf8mb4',
            'passwd': 'Macquest2@',
            'cursorclass':pymysql.cursors.DictCursor
            }

    def get_reviewid_set(self):
        reviewid_set = set()
        sql = "select reviewid from Complete_reviews_new"
        conn = pymysql.connect(**self.config)
        conn.autocommit(1)
        cursor = conn.cursor()
        cursor.execute(sql)
        for i in cursor.fetchall():
            reviewid_set.add(i['reviewid'])
        conn.close()
        return reviewid_set

    def update_mysql(self, pkg):
        conn = pymysql.connect(**self.config)
        cursor = conn.cursor()
        conn.autocommit(1)    
        sql1 = """INSERT INTO Complete_reviews_new(reviewid, stationid, \
                reviewers, contents, score, publish_date) \
                VALUES ('%s','%s','%s','%s','%s','%s')"""\
                % (pkg['reviewid'],pkg['stationid'],pkg['reviewers'],\
                pkg['contents'],pkg['score'],pkg['publish_date'])

        sql2 = """INSERT INTO Complete_reviews_new_daily(reviewid, stationid, \
                reviewers, contents, score, publish_date) \
                VALUES ('%s','%s','%s','%s','%s','%s')"""\
                % (pkg['reviewid'],pkg['stationid'],pkg['reviewers'],\
                pkg['contents'],pkg['score'],pkg['publish_date'])
        
        logger1.info("SQL1 %s"%(sql1))
        logger1.info("SQL2 %s"%(sql2))
        try:
            # Execute the SQL command
            cursor.execute(sql1)
            cursor.execute(sql2)

        except Exception as e:
            # Rollback in case there is any error
            conn.rollback()
            logger1.debug("SQL issues %s--%s"%(str(e)))
            print ("SQL issues %s--%s"%(sql,str(e)))
        conn.close()

if __name__ == '__main__':
    main()

