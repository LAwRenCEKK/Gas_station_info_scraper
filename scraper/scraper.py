# -*- coding: utf-8 -*-
import urllib
import urllib2
import os
import sys
import logging
import time
import datetime
import traceback
import requests
from logging.handlers import TimedRotatingFileHandler
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.firefox.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


sys.dont_write_bytecode = True


class scraper():
    loggerPath = "/home/liu433/gasinfo/loggings/"
    formatter = logging.Formatter('%(asctime)s - %(message)s')


    logger_source1 = logging.getLogger('no_popular_time')
    logger_source1.setLevel(logging.DEBUG)
    fh_source1 = logging.handlers.TimedRotatingFileHandler(loggerPath +
                                                          "no_popular_time.log", when='midnight', interval=1,
                                                          backupCount=10)
    fh_source1.setLevel(logging.DEBUG)
    fh_source1.setFormatter(formatter)
    logger_source1.addHandler(fh_source1)


    logger_source2 = logging.getLogger('close_on_the_day')
    logger_source2.setLevel(logging.DEBUG)
    fh_source2 = logging.handlers.TimedRotatingFileHandler(loggerPath +
                                                          "close_on_the_day.log", when='midnight', interval=1,
                                                          backupCount=10)
    fh_source2.setLevel(logging.DEBUG)
    fh_source2.setFormatter(formatter)
    logger_source2.addHandler(fh_source2)


    logger_source3 = logging.getLogger('not_enough_data')
    logger_source3.setLevel(logging.DEBUG)
    fh_source3 = logging.handlers.TimedRotatingFileHandler(loggerPath +
                                                          "not_enough_data.log", when='midnight', interval=1,
                                                          backupCount=10)
    fh_source3.setLevel(logging.DEBUG)
    fh_source3.setFormatter(formatter)
    logger_source3.addHandler(fh_source3)


    def getWebPage(self, cid, splash_server='localhost:8050', timeout=15, wait=0.5):
        try:
            google_map_addr = 'https://maps.google.com/?cid=' + cid;
            google_map_addr = urllib.quote_plus(google_map_addr)
            request_url = 'http://' + splash_server + '/render.html?url=' + google_map_addr + \
                          '&timeout=' + str(timeout) + '&wait=' + str(wait)
            print (cid)
            response = urllib2.urlopen(request_url)
            html = response.read()
            content = {'status': 0, 'msg': html } 
        except Exception as e:
            content = {'status': 1, 'msg': str(e) }
        return content


    def getGasPrices(self, cid, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            #liveData = soup.find("div", class_="section-popular-times-now-badge")
            #if liveData != None:
            #    print "Station %s has the rt_popular_time info" %cid 
            #    file1 = open('/home/liu433/gasinfo/rtlog/rt_info.log', 'a')
            #    file1.write(cid + '\n')
            #    file1.close()
            prices_list = soup.find_all("div", class_="section-gas-prices-price")
            prices = {}
            for node in prices_list:
                label = node.find("div", class_='section-gas-prices-label').text
                priceinfo = node.find("span").text
                if label != None:
                    prices[label] = priceinfo
            content = {'status': 0, 'msg': prices}
        # Code breaker    
        except Exception as e:
            traceback.print_exc()
            content = {'status': 1, 'msg': str(e)}
        return content


    def busyhourCollectDaily(self, cid, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            week_list = ["Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat"]
            hour_list = ['Start', 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1, 2, 3]
            assert (soup != None)
            week_busy_hours = []

            # Check Point1 : if the station has busyhour infomation
            busyinfo = soup.find("div", class_="section-popular-times-container")
            if busyinfo == None:
                content = {'status': 10, 'msg': "Station %s does't provide popular time info \
                           Click link for more detail: https//maps.google.com/?cid=%s" % (cid, cid)}
                self.logger_source1.info(content)
                return content

            # Check Point2: if the station has 7 days information format. Note: 99% passing rate
            else:
                # dayinfo is a python list with 7 elements and each one contains the info for the current day
                dayinfo = busyinfo.findChildren(recursive=False)
                if len(dayinfo) != 7:
                    content = {'status': 11, 'msg': "Station %s has only %s days' info \
                                Click link for more detail: https//maps.google.com/?cid=%s" % (len(dayinfo), cid)}
                    return content
                else:
                    # Check Point3: if the anyday has special msg like closd or not enough day
                    for i in range(len(dayinfo)):
                        msg = dayinfo[i].find_all("div", class_="section-popular-times-message")
                        check4 = str(msg[0])
                        check5 = str(msg[1])

                        # Ceck Point4: if the station closed on the day
                        if "Closed" in check4:
                            content ={ 'status': 12, 'msg': "Station %s is closed on %s https://maps.google.com/?cid=%s" \
                                       % (cid, week_list[i], cid)}
                            self.logger_source2.info(content)
                            continue

                        # Check Pint5: if the station doesn't have enough date on the day
                        if "Not enough data" in check5:
                            content ={ 'status': 13, 'msg':"Station %s does't have enough date on %s \
                                       https://maps.google.com/?cid=%s" \
                                       % (cid, week_list[i], cid)}
                            self.logger_source3.info(content)
                            continue

                        # This part is to analyze the html file and extract the busyhour info and format it
                        busyhours = dayinfo[i].find_all("div", class_="section-popular-times-bar")
                        busydesc = []
                        for j in range(len(busyhours)):
                            hourinfo = self._parseBusyStr(busyhours[j].attrs['aria-label'], cid)
                            hourinfo.append(week_list[i])
                            busydesc.append(hourinfo)
                        week_busy_hours.append(busydesc)
                    content = {'status': 0, 'msg':week_busy_hours}
                    return content
            # Code breaker    
        except Exception as e:
            traceback.print_exc()
            content = {'status': 1, 'msg':str(e)}
            return content


    def realtimeBusyhourCollect(self, cid, html):
        try:
            week = ['Mon', 'Tue', 'Wed','Thur','Fri', 'Sat', 'Sun']
            week_day = week[datetime.datetime.today().weekday()]
            hour = datetime.datetime.today().hour
            soup = BeautifulSoup(html, 'html.parser')
            # live is the list with everyday's busyhour info in string format
            live = soup.find_all("div", class_="section-popular-times-bar")
            for i in live:
                # livedata core busyhour infor in string format
                livedata = i.attrs['aria-label']

                # filter out none realtime busyhour info
                if len(livedata) > 25:
                    # extract the current freq and usual freq
                    trim = livedata.replace('Currently', '').replace('busy', ''). \
                        replace('usually', '').replace('%', '').replace('.', '').replace(' ', '')
                    realtimedata = trim.strip().split(',')
                    content = {'status':0 ,'msg': \
                              {'week':week_day, 'hour': hour, 'current':realtimedata[0], 'usual':realtimedata[1]}}
                    print content
                    return content
            content = {'status':1 ,'msg': "need to syn up for https://maps.google.com/?cid=%s" % cid}
        except Exception as e:
            traceback.print_exc()
            content = {'status': 1, 'msg': str(e)}
        return content


    def _parseBusyStr(self, busystr, cid):
        try:
            fields = busystr.split()
            busynum = float(fields[0].strip('%')) * 1.0 / 100
            hour = fields[-2] + fields[-1]
            ampm = hour[-5:]
            time = hour[:-5]
            if ampm[0] == 'a':
                hour_f = time + ":00" + "AM"
            else:
                hour_f = time + ":00" + "PM"
            return [hour_f, busynum]
        except ValueError:
            # this is the excetion format: Currently 13% busy, usually 32% busy.
            return ['live', busystr]


    def get_num_photo_review(self, cid, html):
        try:
            soup = BeautifulSoup(html,'html.parser')
            photonodes = soup.find_all("span", class_="gm2-body-2")
            reviewnodes = soup.find_all("button", class_="jqnFjrOWMVU__button gm2-caption")
            ratingnode = soup.find("span", class_="section-star-display")
            try:
                photo = photonodes[1].get_text().split()[0]
            except:
                photo = ''
            try:
                review = reviewnodes[0].get_text().split()[0]
            except:
                review = ''
            try:
                rating =  ratingnode.get_text()
            except:
                rating = ''
            content = {'status': 0, 'msg': {'photo': photo, 'review': review, 'rating': rating}}
        # Code breaker    
        except Exception as e:
            traceback.print_exc()
            content = {'status': 1, 'msg': str(e)}
        return content


    def get_top_reviews(self, cid, html):
        try:
            top_review = []
            soup = BeautifulSoup(html,'html.parser')
            reviewnodes = soup.find_all("div", class_="section-review-snippet-line")
            for i in reviewnodes:
                item = i.get_text().replace('reviewers','').replace("'",'').strip()
                top_review.append(item)
            content = {'status': 0, 'msg': top_review}
            return content
        
        # Code breaker    
        except Exception as e:
            traceback.print_exc()
            content = {'status': 1, 'msg': str(e)}
            return content


    def get_extra_info(self):
    #Collect the Wholesale price information
        try:
            # Retrive the HTML from URL
            f = requests.get('https://www.petro-canada.ca/en/rack-pricing/daily-rack-pricing.aspx')
            soup = BeautifulSoup(f.text, 'html.parser')
            # locate the table and extract information
            lis = soup.find_all('table', class_="rack-pricing__table")

            items = []
            for i in str(lis[0]).split('<tr>')[2:]:
                sub = (
                    i.split('<td class="charttextwhite-s">')[0].replace('<td class=" tableheader-s" style="text-align:left;">',
                                                                     ''))
                sub = sub.replace('<br/>', ' ')
                sub = sub.replace('</td>', '')
                sub = sub.replace('<th>', '')
                sub = sub.replace('</th>', '')
                sub = sub.replace('<td>', '')
                sub = sub.strip()
                items.append(sub)
            for i in range(len(items)):
                items[i] = items[i].split('\n')
            for i in range(len(items)):
                for j in range(len(items[i])):
                    if items[i][j] == '':
                        items[i][j] = '0'
            content = {'status': 0, 'msg': items}
            return content
    
        except Exception as e:
            traceback.print_exc()
            content = {'status':1 ,'msg': str(e)}    
            return content

    def get_station_info(self, cid, html):
        soup = BeautifulSoup(html,'html.parser')
        try:
            try:    
                openhour = soup.find("div", class_="section-open-hours-container section-open-hours-container-hoverable").get_text()
                openhour = openhour.replace('Holiday hours Hours might differ','')
                openhour = openhour.replace('           ','|')
                openhour = openhour.replace('    ',':')[1:-1]
            except AttributeError:
                openhour = '-'
            address = soup.find_all("span", class_="widget-pane-link")[2].get_text()
            brand = soup.find("h1", class_="GLOBAL__gm2-headline-5 section-hero-header-title-title").get_text()
            content = {'status':0 ,'msg': {'stationid': cid, 'brand': brand, 'address': address, 'openhour': openhour}}
        except Exception as e:
            content = {'status':1 ,'msg': "Station: %s - exception: %s"%(cid,str(e))}
        return content


def main():
    start = time.time()
    file = open("/u50/liu433/gasinfo/Dataset_google.txt","r")
    myScrape = scraper()
    cid_list = []
    for i in file.readlines():
        try:
            cid =  i.split("|")[-1].strip()
            cid_list.append(cid)
            html = myScrape.getWebPage(cid)['msg']
            print myScrape.get_station_info(cid, html)
        except Exception as e:
            print str(e)
    end = time.time()
    print(end - start)

if __name__ == '__main__':
    main()
